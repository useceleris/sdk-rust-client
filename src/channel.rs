use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, SystemTime};

use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Bytes;

use crate::client::ClientConfiguration;
use crate::command_queue::{CommandQueue, InterestKind, QueuedPublish};
use crate::connection::{ConnectionState, WebSocket, closed_before_sent};
use crate::connection_url::credential_url;
use crate::constants::{CLOSE_BUDGET, DEFAULT_SEGMENT_ID, RETRY_BUDGET_RESET};
use crate::credentials::{CredentialReason, CredentialRequest, validate_credentials};
use crate::encode::{SegmentCommand, encode_presence_list, encode_publish};
use crate::errors::{CelerisError, configuration_error, failure};
use crate::events::{ChannelEventHandler, EventQueue, Listeners};
use crate::identifiers::identifier_rule;
use crate::ordered_map::OrderedMap;
use crate::reconnect::{replay_lookback, retry_delay};
use crate::segment::{Segment, Subscription};
use crate::values::{ChannelState, PresencePage, RecoveryEvent};

/// One WebSocket connection. Every segment of the channel is multiplexed over
/// it; another `Channel`, even for the same reference, is another connection
/// (SEG-01). Obtain one with [`Client::channel`](crate::Client::channel).
///
/// A `Channel` is cheap to clone, and every clone is the same channel. Close
/// it when done: dropping every handle also stops its connection, without the
/// closing handshake.
#[derive(Clone)]
pub struct Channel {
    pub(crate) shared: Arc<ChannelShared>,
} // end struct Channel

impl fmt::Debug for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Channel")
            .field("reference", &self.shared.reference)
            .field("state", &self.state())
            .finish()
    } // end method fmt
} // end impl Debug for Channel

impl Channel {
    pub(crate) fn new(client: Arc<ClientConfiguration>, reference: &str) -> Channel {
        let shared = Arc::new_cyclic(|this| ChannelShared {
            client: Arc::clone(&client),
            reference: reference.to_owned(),
            inner: Mutex::new(ChannelInner::new(this.clone(), Arc::clone(&client))),
            idle: Notify::new(),
            closed: watch::Sender::new(false),
        });

        Channel { shared }
    } // end fn new

    /// The channel reference that this channel connects to.
    pub fn reference(&self) -> &str {
        &self.shared.reference
    } // end method reference

    /// The current state. It may change as soon as it is read.
    pub fn state(&self) -> ChannelState {
        self.shared.lock().state
    } // end method state

    /// The handler for channel-wide listeners.
    pub fn events(&self) -> ChannelEventHandler {
        ChannelEventHandler {
            channel: Arc::clone(&self.shared),
        }
    } // end method events

    /// A handle for the segment with the given id. It does no network work,
    /// and any number of handles for one segment share its subscriptions and
    /// listeners. It fails with [`CelerisError::Configuration`] for an id that
    /// is empty or contains CR or LF.
    pub fn segment(&self, segment_id: &str) -> Result<Segment, CelerisError> {
        if let Some(rule) = identifier_rule(segment_id) {
            return Err(configuration_error("segment ID", &[failure("", rule)]));
        }

        Ok(Segment::new(Arc::clone(&self.shared), segment_id))
    } // end method segment

    /// A handle for the segment that every connection joins automatically
    /// (SEG-01).
    pub fn default_segment(&self) -> Segment {
        Segment::new(Arc::clone(&self.shared), DEFAULT_SEGMENT_ID)
    } // end method default_segment

    /// Opens the connection. It returns once the WebSocket is established and
    /// the held subscriptions are queued to be restored ahead of any publish,
    /// or fails without a retry: only a connection that was up recovers
    /// automatically.
    ///
    /// It fails with [`CelerisError::OperationInProgress`] while connecting,
    /// connected or reconnecting, and with [`CelerisError::NotConnected`] once
    /// the channel is closing or closed. A failed attempt leaves the channel
    /// [`ChannelState::Failed`], from which `connect` may be called again.
    /// Dropping the returned future abandons the attempt and leaves the channel
    /// failed.
    pub async fn connect(&self) -> Result<(), CelerisError> {
        let channel = &self.shared;

        let generation = {
            let mut inner = channel.lock();
            inner.runtime.get_or_insert_with(Handle::current);

            match inner.state {
                ChannelState::Connecting | ChannelState::Connected | ChannelState::Reconnecting => {
                    return Err(CelerisError::OperationInProgress(format!(
                        "Connect was already called; the channel is {}.",
                        inner.state
                    )));
                }
                ChannelState::Closing | ChannelState::Closed => return Err(closed_channel()),
                ChannelState::Idle | ChannelState::Failed => {}
            }

            inner.generation += 1;
            inner.retries_used = 0;
            inner.outage = None;
            inner.deduplication.clear();
            inner.queue_state_change(ChannelState::Connecting);
            inner.generation
        };

        channel.dispatch_events();

        let abandoned = AbandonConnect {
            channel: Some(Arc::clone(channel)),
            generation,
        };

        let result = channel
            .establish_connection(generation, CredentialReason::Initial, 0)
            .await;

        abandoned.disarm();

        if result.is_err() {
            channel.lock().fail_initial_connect(generation);
        }

        channel.dispatch_events();

        result
    } // end method connect

    /// Closes the connection and every segment handle with it. It is terminal
    /// and idempotent, and returns within about five seconds even when the
    /// server does not answer. Publishes and presence queries that still wait
    /// fail with [`CelerisError::Cancelled`].
    pub async fn close(&self) {
        let channel = &self.shared;

        let closing = {
            let mut inner = channel.lock();
            inner.runtime.get_or_insert_with(Handle::current);
            inner.begin_close()
        };

        let CloseStart::Started(closing) = closing else {
            let mut closed = channel.closed.subscribe();
            let _ = closed.wait_for(|closed| *closed).await;

            return;
        };

        channel.dispatch_events();

        if let Some(ClosingConnection { id, writer }) = closing {
            // The writer flushes what it holds, then performs the closing
            // handshake, within the budget.
            if let Some(mut writer) = writer
                && timeout(CLOSE_BUDGET, &mut writer).await.is_err()
            {
                writer.abort();
            }

            channel.lock().discard_connection(id, &closed_before_sent());
        }

        channel.lock().queue_state_change(ChannelState::Closed);
        channel.closed.send_replace(true);
        channel.dispatch_events();
    } // end method close
} // end impl Channel

/// What a call to close found.
enum CloseStart {
    AlreadyStarted,

    /// This call closes, and finishes the connection that it detached.
    Started(Option<ClosingConnection>),
} // end enum CloseStart

/// A connection that flushes and closes, and its writer.
struct ClosingConnection {
    id: u64,
    writer: Option<JoinHandle<()>>,
} // end struct ClosingConnection

/// Fails the initial attempt when the caller drops the future of `connect`.
struct AbandonConnect {
    channel: Option<Arc<ChannelShared>>,
    generation: u64,
} // end struct AbandonConnect

impl AbandonConnect {
    /// The attempt finished, so nothing is abandoned.
    fn disarm(mut self) {
        self.channel = None;
    } // end method disarm
} // end impl AbandonConnect

impl Drop for AbandonConnect {
    fn drop(&mut self) {
        let Some(channel) = self.channel.take() else {
            return;
        };

        let mut inner = channel.lock();

        if inner.generation == self.generation {
            // Dropping the sender cancels the attempt in flight.
            inner.attempt_cancel = None;
        }

        inner.fail_initial_connect(self.generation);
        drop(inner);
        channel.dispatch_events();
    } // end method drop
} // end impl Drop for AbandonConnect

fn closed_channel() -> CelerisError {
    CelerisError::NotConnected(
        "Channel is closed; create a new one with Client::channel.".to_owned(),
    )
} // end fn closed_channel

fn cancelled_attempt() -> CelerisError {
    CelerisError::Cancelled(
        "Connection attempt cancelled: the channel was closed or a newer attempt started."
            .to_owned(),
    )
} // end fn cancelled_attempt

/// What every handle of one channel shares.
pub(crate) struct ChannelShared {
    pub(crate) client: Arc<ClientConfiguration>,
    pub(crate) reference: String,
    inner: Mutex<ChannelInner>,

    /// Signalled whenever the event queue becomes quiet.
    pub(crate) idle: Notify,

    /// Becomes `true` when close finishes.
    closed: watch::Sender<bool>,
} // end struct ChannelShared

impl ChannelShared {
    /// The channel's one lock. It guards all of the channel's state and is
    /// never held while user code runs or while the socket blocks. A listener
    /// that panics never holds it, so the lock is used even if poisoned.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ChannelInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    } // end method lock
} // end impl ChannelShared

/// A pending timer. Its token tells a firing timer whether it is still the
/// current one, since a timer can fire just as it is stopped.
pub(crate) struct Timer {
    pub(crate) token: u64,
    task: JoinHandle<()>,
} // end struct Timer

impl Timer {
    pub(crate) fn stop(self) {
        self.task.abort();
    } // end method stop
} // end impl Timer

/// When the connection dropped: a monotonic instant measures the outage, and
/// the wall clock goes to the credential provider.
#[derive(Clone, Copy)]
struct Outage {
    started: Instant,
    wall: SystemTime,
} // end struct Outage

pub(crate) struct PendingPresence {
    request_id: String,
    result: oneshot::Sender<Result<PresencePage, CelerisError>>,
    timer: Timer,
} // end struct PendingPresence

/// The channel's state, guarded by its lock.
pub(crate) struct ChannelInner {
    pub(crate) this: Weak<ChannelShared>,
    pub(crate) client: Arc<ClientConfiguration>,

    /// The runtime that the first connect or close ran on. Tasks and listener
    /// deliveries run on it.
    pub(crate) runtime: Option<Handle>,

    pub(crate) state: ChannelState,
    pub(crate) generation: u64,

    /// The current connection and any connection that still closes.
    pub(crate) connections: Vec<ConnectionState>,
    pub(crate) current_connection: Option<u64>,
    pub(crate) last_connection_id: u64,

    retries_used: u32,
    retry_timer: Option<Timer>,
    connected_at: Instant,
    outage: Option<Outage>,

    /// Cancels the attempt in flight when dropped.
    attempt_cancel: Option<oneshot::Sender<()>>,

    close_started: bool,

    pub(crate) queue: CommandQueue,
    deduplication: DeduplicationWindow,

    message_interests: OrderedMap<usize>,
    presence_interests: OrderedMap<usize>,

    /// Issues presence query request ids. Kept per channel rather than per
    /// socket, so an id is never reused across reconnects (QUERY-01).
    presence_request_count: u64,
    pending_presence: Option<PendingPresence>,

    pub(crate) listeners: Listeners,
    pub(crate) events: EventQueue,

    last_timer_token: u64,
} // end struct ChannelInner

impl ChannelInner {
    fn new(this: Weak<ChannelShared>, client: Arc<ClientConfiguration>) -> ChannelInner {
        ChannelInner {
            this,
            client,
            runtime: None,
            state: ChannelState::Idle,
            generation: 0,
            connections: Vec::new(),
            current_connection: None,
            last_connection_id: 0,
            retries_used: 0,
            retry_timer: None,
            connected_at: Instant::now(),
            outage: None,
            attempt_cancel: None,
            close_started: false,
            queue: CommandQueue::default(),
            deduplication: DeduplicationWindow::default(),
            message_interests: OrderedMap::default(),
            presence_interests: OrderedMap::default(),
            presence_request_count: 0,
            pending_presence: None,
            listeners: Listeners::default(),
            events: EventQueue::default(),
            last_timer_token: 0,
        }
    } // end fn new

    /// The runtime of the first connect or close. Everything that needs it
    /// runs only after one of them.
    pub(crate) fn runtime(&self) -> Handle {
        self.runtime
            .clone()
            .expect("the channel's runtime is known once it connects or closes")
    } // end method runtime

    /// Starts a timer that calls `fire` with its token, then delivers the
    /// events that `fire` queued.
    pub(crate) fn start_timer(
        &mut self,
        delay: Duration,
        fire: fn(&Arc<ChannelShared>, u64),
    ) -> Timer {
        self.last_timer_token += 1;
        let token = self.last_timer_token;
        let channel = self.this.clone();

        let task = self.runtime().spawn(async move {
            sleep(delay).await;

            if let Some(channel) = channel.upgrade() {
                fire(&channel, token);
                channel.dispatch_events();
            }
        });

        Timer { token, task }
    } // end method start_timer

    /// Starts closing: everything that waits is cancelled, and the current
    /// connection, if any, is asked to flush and close.
    fn begin_close(&mut self) -> CloseStart {
        if self.close_started {
            return CloseStart::AlreadyStarted;
        }

        self.close_started = true;
        self.reject_pending_presence(CelerisError::Cancelled(
            "Channel closed while the presence query was pending.".to_owned(),
        ));
        self.generation += 1;
        self.stop_retry_timer();

        // Dropping the sender cancels the attempt in flight.
        self.attempt_cancel = None;
        self.reset_queue(&closed_before_sent());

        let closing = self.current_connection.take().and_then(|id| {
            let connection = self.connection_mut(id)?;
            connection.begin_closing();

            Some(ClosingConnection {
                id,
                writer: connection.writer.take(),
            })
        });

        self.queue_state_change(ChannelState::Closing);

        CloseStart::Started(closing)
    } // end method begin_close

    fn stop_retry_timer(&mut self) {
        if let Some(timer) = self.retry_timer.take() {
            timer.stop();
        }
    } // end method stop_retry_timer

    /// Marks the initial attempt of `generation` as failed, unless a close or a
    /// newer attempt overtook it.
    fn fail_initial_connect(&mut self, generation: u64) {
        if generation == self.generation {
            self.generation += 1;
            self.queue_state_change(ChannelState::Failed);
        }
    } // end method fail_initial_connect

    /// Starts recovery when the channel's own socket closed or failed, and
    /// reports if that queued events.
    pub(crate) fn receive_socket_failure(&mut self, id: u64) -> bool {
        if self.current_connection != Some(id) {
            return false;
        }

        self.enter_reconnecting(Instant::now());

        true
    } // end method receive_socket_failure

    /// Starts recovery when the heartbeat found the channel's own socket dead,
    /// and reports if that queued events. The outage began when the server
    /// was last heard from, so the replay lookback covers what was published
    /// during the silence.
    pub(crate) fn heartbeat_failure(&mut self, id: u64) -> bool {
        if self.current_connection != Some(id) {
            return false;
        }

        let last_heard = self
            .connection(id)
            .map_or_else(Instant::now, |connection| connection.heartbeat.last_heard);

        self.enter_reconnecting(last_heard);

        true
    } // end method heartbeat_failure

    /// Starts recovery from an outage that began at `outage_start`.
    fn enter_reconnecting(&mut self, outage_start: Instant) {
        self.reject_pending_presence(connection_lost_during_query());
        self.drop_queue_connection();

        let now = Instant::now();

        if now.duration_since(self.connected_at) >= RETRY_BUDGET_RESET {
            self.retries_used = 0;
        }

        // The wall clock moves back by the same span as the monotonic one.
        let elapsed = now.duration_since(outage_start);

        self.outage = Some(Outage {
            started: outage_start,
            wall: SystemTime::now() - elapsed,
        });

        if let Some(id) = self.current_connection {
            let unwritten = self
                .connection_mut(id)
                .map(ConnectionState::take_unwritten)
                .unwrap_or_default();

            self.put_back(unwritten);
            self.discard_connection(id, &closed_before_sent());
        }

        self.queue_state_change(ChannelState::Reconnecting);
        self.schedule_retry();
    } // end method enter_reconnecting

    fn schedule_retry(&mut self) {
        let delay = retry_delay(self.retries_used, (self.client.random)());
        self.retry_timer = Some(self.start_timer(delay, ChannelShared::retry_elapsed));
    } // end method schedule_retry

    fn fail_terminal(&mut self, error: CelerisError) {
        self.reject_pending_presence(connection_lost_during_query());
        self.reset_queue(&error);
        self.generation += 1;
        self.stop_retry_timer();
        self.queue_error(error);
        self.queue_state_change(ChannelState::Failed);
    } // end method fail_terminal

    pub(crate) fn interests(&self, kind: InterestKind) -> &OrderedMap<usize> {
        match kind {
            InterestKind::Message => &self.message_interests,
            InterestKind::Presence => &self.presence_interests,
        }
    } // end method interests

    fn interests_mut(&mut self, kind: InterestKind) -> &mut OrderedMap<usize> {
        match kind {
            InterestKind::Message => &mut self.message_interests,
            InterestKind::Presence => &mut self.presence_interests,
        }
    } // end method interests_mut

    /// Counts one interest; the first registration queues a sync of the
    /// segment's subscription.
    pub(crate) fn add_interest(
        &mut self,
        kind: InterestKind,
        segment_id: &str,
    ) -> Result<(), CelerisError> {
        if matches!(self.state, ChannelState::Closing | ChannelState::Closed) {
            return Err(closed_channel());
        }

        let count = self.interests(kind).get(segment_id).copied().unwrap_or(0);
        self.interests_mut(kind).set(segment_id, count + 1);

        if count == 0 {
            self.queue_interest_sync(kind, segment_id);
        }

        Ok(())
    } // end method add_interest

    /// Releases one interest; the last cancellation queues a sync of the
    /// segment's subscription.
    pub(crate) fn release_interest(&mut self, kind: InterestKind, segment_id: &str) {
        let count = self.interests(kind).get(segment_id).copied().unwrap_or(0);

        if count > 1 {
            self.interests_mut(kind).set(segment_id, count - 1);

            return;
        }

        self.interests_mut(kind).remove(segment_id);
        self.queue_interest_sync(kind, segment_id);
    } // end method release_interest

    /// Does nothing before the first socket: installing one syncs every held
    /// interest. While reconnecting the change waits in the queue, behind the
    /// publishes queued before it.
    fn queue_interest_sync(&mut self, kind: InterestKind, segment_id: &str) {
        if self.current_connection.is_some() || self.state == ChannelState::Reconnecting {
            self.queue_interest(kind, segment_id);
        }
    } // end method queue_interest_sync

    /// Names the command that brings the server in line with the segment's
    /// interest as it stands now, or `None` when none is needed. Subscriptions
    /// are synced as state, so a resend is always safe.
    pub(crate) fn interest_command(
        &self,
        kind: InterestKind,
        segment_id: &str,
    ) -> Option<SegmentCommand> {
        let held = self.interests(kind).get(segment_id).is_some();

        match kind {
            // Presence applies to every segment, the default one included: the
            // server's connect-time join grants message membership only.
            InterestKind::Presence if held => Some(SegmentCommand::PresenceSubscribe),
            InterestKind::Presence => Some(SegmentCommand::PresenceUnsubscribe),

            // The server joins the default segment on connect and never leaves
            // it.
            InterestKind::Message if segment_id == DEFAULT_SEGMENT_ID => None,

            // Watching presence is not membership, so it never holds the
            // segment.
            InterestKind::Message if held => Some(SegmentCommand::Subscribe),
            InterestKind::Message => Some(SegmentCommand::Unsubscribe),
        }
    } // end method interest_command

    fn take_pending_presence(&mut self) -> Option<PendingPresence> {
        let pending = self.pending_presence.take()?;
        pending.timer.task.abort();

        Some(pending)
    } // end method take_pending_presence

    pub(crate) fn reject_pending_presence(&mut self, error: CelerisError) {
        if let Some(pending) = self.take_pending_presence() {
            let _ = pending.result.send(Err(error));
        }
    } // end method reject_pending_presence

    /// Settles the pending query whose request id is `request_id`. A reply for
    /// any other id answers a query that already failed, so it is dropped.
    pub(crate) fn settle_pending_presence(
        &mut self,
        request_id: &str,
        result: Result<PresencePage, CelerisError>,
    ) {
        let pending_matches = self
            .pending_presence
            .as_ref()
            .is_some_and(|pending| pending.request_id == request_id);

        if pending_matches && let Some(pending) = self.take_pending_presence() {
            let _ = pending.result.send(result);
        }
    } // end method settle_pending_presence

    /// Records a delivered id. Returns `false` for an id already in the window.
    pub(crate) fn record_delivery(&mut self, message_id: &str) -> bool {
        self.deduplication
            .record_if_new(message_id, self.client.deduplication_window_size)
    } // end method record_delivery
} // end impl ChannelInner

fn connection_lost_during_query() -> CelerisError {
    CelerisError::Transport(
        "Connection lost during the presence query; query again once the channel reconnects."
            .to_owned(),
    )
} // end fn connection_lost_during_query

impl ChannelShared {
    /// Runs one attempt: fresh credentials, the handshake, then installing the
    /// socket. On success the channel is connected and, for a reconnect, the
    /// recovery event is queued, all before any received message is routed.
    async fn establish_connection(
        self: &Arc<Self>,
        generation: u64,
        reason: CredentialReason,
        retry_index: u32,
    ) -> Result<(), CelerisError> {
        let reconnect = matches!(reason, CredentialReason::Reconnect { .. });

        let deadline_span = if reconnect {
            self.client.reconnect_timeout
        } else {
            self.client.connect_timeout
        };

        let deadline = Instant::now() + deadline_span;
        let (cancel, cancelled) = oneshot::channel();

        {
            let mut inner = self.lock();

            if generation != inner.generation {
                return Err(cancelled_attempt());
            }

            inner.attempt_cancel = Some(cancel);
        }

        let request = CredentialRequest {
            channel_reference: self.reference.clone(),
            reason,
        };

        let opened = tokio::select! {
            biased;
            _ = cancelled => Err(cancelled_attempt()),
            () = sleep_until(deadline) => Err(CelerisError::Timeout(format!(
                "Connection attempt timed out after {} ms.",
                deadline_span.as_millis()
            ))),
            opened = self.open_socket(request) => opened,
        };

        let mut inner = self.lock();
        inner.attempt_cancel = None;
        let socket = opened?;

        if generation != inner.generation {
            return Err(cancelled_attempt());
        }

        inner.install_connection(socket);
        inner.connected_at = Instant::now();
        inner.outage = None;

        // Restoration goes through the queue, so it waits for writer room and
        // reaches the server before the publishes that waited for this socket.
        inner.restore_interests();
        inner.queue_state_change(ChannelState::Connected);

        if reconnect {
            let recovery = RecoveryEvent {
                retry_index,
                possible_gaps: true,
                possible_duplicates: true,
            };

            inner.queue_event(
                |listeners| Some(&listeners.recoveries),
                move |listener| listener(&recovery),
                true,
            );
        }

        Ok(())
    } // end method establish_connection

    /// Acquires credentials and performs the handshake. Every failure maps to
    /// fixed text: a provider's error and the dialer's error, which quotes the
    /// credential URL, are never passed on.
    async fn open_socket(&self, request: CredentialRequest) -> Result<WebSocket, CelerisError> {
        // A task of its own contains a provider's panic; aborting it when this
        // future is dropped cancels the provider.
        let mut provider = AbortOnDrop(tokio::spawn(
            self.client.credential_provider.provide(request),
        ));

        let credentials = match (&mut provider.0).await {
            Ok(Ok(credentials)) => credentials,
            Ok(Err(_)) | Err(_) => {
                return Err(CelerisError::Transport(
                    "Credential acquisition failed: the credential provider returned an error or panicked."
                        .to_owned(),
                ));
            }
        };

        validate_credentials(&credentials)?;

        let url = credential_url(&self.client.base_url, &self.reference, &credentials);

        (self.client.dial)(url).await.map_err(|()| {
            CelerisError::Transport(
                "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference."
                    .to_owned(),
            )
        })
    } // end method open_socket

    /// The retry delay is over: run the next reconnect attempt.
    fn retry_elapsed(self: &Arc<Self>, token: u64) {
        let generation = {
            let mut inner = self.lock();

            if !inner
                .retry_timer
                .as_ref()
                .is_some_and(|timer| timer.token == token)
            {
                return;
            }

            inner.retry_timer = None;
            inner.generation
        };

        tokio::spawn(Arc::clone(self).run_reconnect_attempt(generation));
    } // end method retry_elapsed

    async fn run_reconnect_attempt(self: Arc<Self>, generation: u64) {
        let (retry_index, reason) = {
            let inner = self.lock();

            let Some(outage) = inner.outage.filter(|_| generation == inner.generation) else {
                return;
            };

            let reason = CredentialReason::Reconnect {
                disconnected_at: outage.wall,
                replay_lookback: replay_lookback(outage.started.elapsed()),
            };

            (inner.retries_used, reason)
        };

        let result = self
            .establish_connection(generation, reason, retry_index)
            .await;

        if let Err(error) = result {
            let mut inner = self.lock();

            if generation == inner.generation {
                if matches!(error, CelerisError::Transport(_) | CelerisError::Timeout(_)) {
                    inner.retries_used += 1;

                    if inner.retries_used >= self.client.maximum_reconnect_attempts {
                        inner.fail_terminal(error);
                    } else {
                        inner.schedule_retry();
                    }
                } else {
                    inner.fail_terminal(error);
                }
            }
        }

        self.dispatch_events();
    } // end method run_reconnect_attempt

    pub(crate) fn subscribe(
        self: &Arc<Self>,
        kind: InterestKind,
        segment_id: &str,
    ) -> Result<Subscription, CelerisError> {
        self.lock().add_interest(kind, segment_id)?;

        Ok(Subscription::new(Arc::clone(self), kind, segment_id))
    } // end method subscribe

    pub(crate) async fn publish(
        self: &Arc<Self>,
        segment_id: &str,
        payload: &[u8],
        message_id: &str,
    ) -> Result<(), CelerisError> {
        // Encoding copies up to 2 MiB, so it happens without the lock. Its
        // failure is still reported in the reference's order, after the
        // connection check.
        let encoded = encode_publish(segment_id, Some(message_id), payload);

        let (publish, result) = {
            let mut inner = self.lock();

            // While reconnecting the publish waits in the queue for the next
            // socket (QUEUE-01).
            let connected =
                inner.state == ChannelState::Connected && inner.current_connection.is_some();

            if !connected && inner.state != ChannelState::Reconnecting {
                return Err(CelerisError::NotConnected(format!(
                    "Channel is not connected; it is {}.",
                    inner.state
                )));
            }

            inner.queue_publish(segment_id, message_id, encoded?)?
        };

        // Dropping this future takes the publish back; once it settled, that
        // does nothing.
        let _withdraw = WithdrawPublish {
            channel: Arc::clone(self),
            publish,
        };

        result.await.unwrap_or_else(|_| Err(closed_before_sent()))
    } // end method publish

    /// Settles a publish whose caller stopped waiting. A rate limit can requeue
    /// a publish still in the writer, so it may have a copy queued and copies
    /// in the writer at once: every copy not yet written is taken back. No
    /// later rate limit resends it.
    fn cancel_publish(&self, publish: &Arc<QueuedPublish>) {
        let mut inner = self.lock();

        if publish.is_settled() {
            return;
        }

        let connection_id = publish.connection();
        let withdrawn = inner.withdraw_publish(publish);

        let (removed, writing) = connection_id
            .and_then(|id| inner.connection_mut(id))
            .map_or((false, false), |connection| {
                connection.remove_publish(publish)
            });

        if writing {
            publish.settle(Err(CelerisError::DeliveryUnknown(
                "The publish was dropped while the socket was writing it, so it may or may not have been sent."
                    .to_owned(),
            )));
        } else if withdrawn || removed {
            publish.settle(Err(CelerisError::Cancelled(
                "The publish was dropped before it was sent.".to_owned(),
            )));
        } else {
            return;
        }

        inner.forget_publish(publish);

        // The room taken back from the writer may let a waiting command through.
        if removed && connection_id == inner.current_connection {
            inner.drain();
        }
    } // end method cancel_publish

    pub(crate) async fn query_presence(
        self: &Arc<Self>,
        segment_id: &str,
        page: i32,
        per_page: i32,
    ) -> Result<PresencePage, CelerisError> {
        let (request_id, result) = {
            let mut inner = self.lock();

            let connection_id = match inner.current_connection {
                Some(id) if inner.state == ChannelState::Connected => id,
                _ => {
                    return Err(CelerisError::NotConnected(format!(
                        "Channel is not connected; it is {}.",
                        inner.state
                    )));
                }
            };

            if inner.pending_presence.is_some() {
                return Err(CelerisError::OperationInProgress(
                    "A presence query is already in flight; wait for it to settle before starting another."
                        .to_owned(),
                ));
            }

            inner.presence_request_count += 1;
            let request_id = inner.presence_request_count.to_string();
            let data: Bytes = encode_presence_list(segment_id, page, per_page, &request_id)?;

            // A query that the writer refuses fails without taking the slot.
            inner.send_now(connection_id, data)?;

            // A timed-out or dropped query frees its slot and leaves the
            // connection alone: a late reply carries the old request id and is
            // dropped.
            let (sender, receiver) = oneshot::channel();
            let timer = inner.start_timer(
                self.client.presence_query_timeout,
                ChannelShared::presence_timed_out,
            );

            inner.pending_presence = Some(PendingPresence {
                request_id: request_id.clone(),
                result: sender,
                timer,
            });

            (request_id, receiver)
        };

        // Dropping this future frees the slot; once the query settled, that
        // does nothing.
        let _abandon = AbandonPresenceQuery {
            channel: Arc::clone(self),
            request_id,
        };

        result
            .await
            .unwrap_or_else(|_| Err(connection_lost_during_query()))
    } // end method query_presence

    fn presence_timed_out(self: &Arc<Self>, token: u64) {
        let mut inner = self.lock();

        let current = inner
            .pending_presence
            .as_ref()
            .is_some_and(|pending| pending.timer.token == token);

        if current {
            let error = CelerisError::Timeout(format!(
                "Presence query timed out after {} ms.",
                self.client.presence_query_timeout.as_millis()
            ));

            inner.reject_pending_presence(error);
        }
    } // end method presence_timed_out
} // end impl ChannelShared

/// Takes back a publish when the caller drops the future of `publish`.
struct WithdrawPublish {
    channel: Arc<ChannelShared>,
    publish: Arc<QueuedPublish>,
} // end struct WithdrawPublish

impl Drop for WithdrawPublish {
    fn drop(&mut self) {
        self.channel.cancel_publish(&self.publish);
    } // end method drop
} // end impl Drop for WithdrawPublish

/// Frees the query slot when the caller drops the future of a presence query.
struct AbandonPresenceQuery {
    channel: Arc<ChannelShared>,
    request_id: String,
} // end struct AbandonPresenceQuery

impl Drop for AbandonPresenceQuery {
    fn drop(&mut self) {
        let mut inner = self.channel.lock();
        let cancelled = CelerisError::Cancelled("The presence query was dropped.".to_owned());
        inner.settle_pending_presence(&self.request_id, Err(cancelled));
    } // end method drop
} // end impl Drop for AbandonPresenceQuery

/// Aborts a task when dropped.
struct AbortOnDrop<Output>(JoinHandle<Output>);

impl<Output> Drop for AbortOnDrop<Output> {
    fn drop(&mut self) {
        self.0.abort();
    } // end method drop
} // end impl Drop for AbortOnDrop

/// Remembers the most recently delivered message ids, a window of them. It
/// survives reconnects, which absorbs replay duplicates, and clears on connect
/// (REV-01).
#[derive(Default)]
struct DeduplicationWindow {
    identifiers: HashSet<String>,
    order: VecDeque<String>,
} // end struct DeduplicationWindow

impl DeduplicationWindow {
    /// Returns `false` for an id already in the window. A new one is recorded,
    /// and the oldest leaves once the window holds `limit` ids.
    fn record_if_new(&mut self, identifier: &str, limit: usize) -> bool {
        if self.identifiers.contains(identifier) {
            return false;
        }

        self.identifiers.insert(identifier.to_owned());
        self.order.push_back(identifier.to_owned());

        if self.order.len() > limit
            && let Some(oldest) = self.order.pop_front()
        {
            self.identifiers.remove(&oldest);
        }

        true
    } // end method record_if_new

    fn clear(&mut self) {
        self.identifiers.clear();
        self.order.clear();
    } // end method clear
} // end impl DeduplicationWindow
