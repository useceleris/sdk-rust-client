use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Bytes;

use crate::channel::{ChannelInner, ChannelShared, Timer};
use crate::constants::{
    MAXIMUM_CONSECUTIVE_RATE_LIMITS, MAXIMUM_PENDING_COMMANDS, MAXIMUM_PUBLISH_RESENDS,
    QUOTA_PROBE_FIRST_DELAY, QUOTA_PROBE_MAXIMUM_DELAY, QUOTA_RETURN_CONFIRMATION,
    RATE_LIMIT_COOLDOWN, RATE_LIMIT_SUSPECT_WINDOW,
};
use crate::encode::encode_segment_command;
use crate::errors::CelerisError;
use crate::ordered_map::OrderedMap;
use crate::reconnect::retry_delay;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InterestKind {
    Message,
    Presence,
} // end enum InterestKind

pub(crate) const INTEREST_KINDS: [InterestKind; 2] =
    [InterestKind::Message, InterestKind::Presence];

pub(crate) type PublishResult = Result<(), CelerisError>;

/// One publish, from the call until it settles. Its progress is changed only
/// while the channel's lock is held.
pub(crate) struct QueuedPublish {
    pub(crate) segment_id: String,
    pub(crate) message_id: String,
    pub(crate) data: Bytes,

    /// Its place in the order that commands were queued in.
    pub(crate) sequence: u64,

    progress: Mutex<PublishProgress>,
} // end struct QueuedPublish

struct PublishProgress {
    resends: u32,

    /// The connection it was last handed to, or `None` while it is queued.
    connection: Option<u64>,

    /// Receives the outcome once; `None` once settled.
    result: Option<oneshot::Sender<PublishResult>>,
} // end struct PublishProgress

impl QueuedPublish {
    fn progress(&self) -> MutexGuard<'_, PublishProgress> {
        self.progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    } // end method progress

    pub(crate) fn settle(&self, result: PublishResult) {
        if let Some(sender) = self.progress().result.take() {
            // The caller may have stopped waiting; then nobody needs the result.
            let _ = sender.send(result);
        }
    } // end method settle

    pub(crate) fn is_settled(&self) -> bool {
        self.progress().result.is_none()
    } // end method is_settled

    pub(crate) fn connection(&self) -> Option<u64> {
        self.progress().connection
    } // end method connection

    pub(crate) fn set_connection(&self, connection: Option<u64>) {
        self.progress().connection = connection;
    } // end method set_connection
} // end impl QueuedPublish

struct SentInterest {
    kind: InterestKind,
    segment_id: String,
    sent_at: Instant,
} // end struct SentInterest

struct SentPublish {
    publish: Arc<QueuedPublish>,
    sent_at: Instant,
} // end struct SentPublish

/// Sends subscription changes ahead of publishes, waits for room in the
/// writer, and resends recent commands after a rate limit (RESEND-01). The
/// server never says which frame a rate limit dropped, so everything sent
/// within the suspect window is resent: subscriptions as their current state,
/// publishes once and with their original message id, so receivers drop any
/// copy that got through.
///
/// It belongs to its channel and is guarded by the channel's lock.
#[derive(Default)]
pub(crate) struct CommandQueue {
    /// Per kind: segment → the sequence of its latest change, in the order the
    /// segments were first marked.
    pending_interests: [OrderedMap<u64>; 2],

    publishes: Vec<Arc<QueuedPublish>>,
    sequence: u64,

    recent_interests: Vec<SentInterest>,

    /// Holds payloads, so it keeps no more than MAXIMUM_PENDING_COMMANDS.
    recent_publishes: Vec<SentPublish>,

    pause_timer: Option<Timer>,
    rate_limit_streak: u32,
    rate_limit_streak_ends_at: Option<Instant>,

    /// Subscriptions dropped while the limit was treated as a used-up quota,
    /// re-sent by a probe on a slow, doubling schedule.
    abandoned_interests: [OrderedMap<()>; 2],
    probe_timer: Option<Timer>,
    probe_count: u32,

    /// `None` until a command is sent after the latest rate limit.
    first_sent_since_rate_limit_at: Option<Instant>,
} // end struct CommandQueue

impl CommandQueue {
    pub(crate) fn is_paused(&self) -> bool {
        self.pause_timer.is_some()
    } // end method is_paused
} // end impl CommandQueue

fn index(kind: InterestKind) -> usize {
    match kind {
        InterestKind::Message => 0,
        InterestKind::Presence => 1,
    }
} // end fn index

impl ChannelInner {
    pub(crate) fn queue_interest(&mut self, kind: InterestKind, segment_id: &str) {
        self.mark_interest(kind, segment_id);
        self.drain();
    } // end method queue_interest

    /// Queues a publish, which settles once written to the socket.
    pub(crate) fn queue_publish(
        &mut self,
        segment_id: &str,
        message_id: &str,
        data: Bytes,
    ) -> Result<(Arc<QueuedPublish>, oneshot::Receiver<PublishResult>), CelerisError> {
        let limit = self.client.publish_queue_size;

        if self.queue.publishes.len() >= limit {
            return Err(CelerisError::Backpressure(format!(
                "The publish queue is full (size {limit}). Retry once some publishes have gone out."
            )));
        }

        let (sender, receiver) = oneshot::channel();
        self.queue.sequence += 1;

        let publish = Arc::new(QueuedPublish {
            segment_id: segment_id.to_owned(),
            message_id: message_id.to_owned(),
            data,
            sequence: self.queue.sequence,
            progress: Mutex::new(PublishProgress {
                resends: 0,
                connection: None,
                result: Some(sender),
            }),
        });

        self.queue.publishes.push(Arc::clone(&publish));
        self.drain();

        Ok((publish, receiver))
    } // end method queue_publish

    /// Removes a publish that has not been handed to the writer, and reports
    /// if it did.
    pub(crate) fn withdraw_publish(&mut self, publish: &Arc<QueuedPublish>) -> bool {
        let before = self.queue.publishes.len();
        self.queue
            .publishes
            .retain(|queued| !Arc::ptr_eq(queued, publish));

        self.queue.publishes.len() < before
    } // end method withdraw_publish

    /// Hands over a command that is never queued or resent, such as a presence
    /// query.
    pub(crate) fn send_now(&mut self, connection_id: u64, data: Bytes) -> Result<(), CelerisError> {
        if self.queue.is_paused() {
            return Err(CelerisError::Backpressure(
                "Sending is paused after a rate limit; try again in a moment.".to_owned(),
            ));
        }

        let has_room = self
            .connection(connection_id)
            .is_some_and(|connection| connection.has_room(data.len()));

        if !has_room {
            return Err(CelerisError::Backpressure(
                "Command writer is full: 64 commands are waiting to be sent. Retry once the socket has flushed them."
                    .to_owned(),
            ));
        }

        self.hand_off(connection_id, data, None);

        Ok(())
    } // end method send_now

    pub(crate) fn receive_rate_limit(&mut self) {
        // A limit that arrives while sending is paused, with nothing sent since
        // the last one, reports the same episode through another limit type
        // (the server throttles each type separately). It carries nothing new.
        if self.queue.is_paused() && self.queue.first_sent_since_rate_limit_at.is_none() {
            return;
        }

        let now = Instant::now();
        self.end_probing_if_quota_returned(now, QUOTA_RETURN_CONFIRMATION);

        // While probing the streak holds, so a probe's own limit cannot start
        // another full run of resends.
        let streak_ended = self
            .queue
            .rate_limit_streak_ends_at
            .is_none_or(|ends_at| now > ends_at);

        if self.queue.probe_count == 0 && streak_ended {
            self.queue.rate_limit_streak = 0;
        }

        // A limit that keeps returning is a used-up quota rather than a burst,
        // and resending into it would never succeed.
        if self.queue.rate_limit_streak < MAXIMUM_CONSECUTIVE_RATE_LIMITS {
            self.requeue_recent(now);
        } else {
            self.abandon_recent();
        }

        self.queue.recent_interests.clear();
        self.queue.recent_publishes.clear();
        self.queue.first_sent_since_rate_limit_at = None;

        let delay =
            RATE_LIMIT_COOLDOWN + retry_delay(self.queue.rate_limit_streak, (self.client.random)());
        self.queue.rate_limit_streak += 1;
        self.queue.rate_limit_streak_ends_at = Some(now + delay + RATE_LIMIT_SUSPECT_WINDOW);

        if let Some(timer) = self.queue.pause_timer.take() {
            timer.stop();
        }

        self.queue.pause_timer = Some(self.start_timer(delay, ChannelShared::pause_elapsed));
    } // end method receive_rate_limit

    /// Fails every waiting publish with `error` and empties the queue, for a
    /// channel that stopped in failed or closed: nothing waits for a later
    /// connect (QUEUE-01).
    pub(crate) fn reset_queue(&mut self, error: &CelerisError) {
        for publish in self.queue.publishes.drain(..) {
            publish.settle(Err(error.clone()));
        }

        for pending in &mut self.queue.pending_interests {
            pending.clear();
        }

        self.drop_queue_connection();
    } // end method reset_queue

    /// Forgets everything tied to the socket that dropped and keeps what still
    /// waits to be sent (QUEUE-01): publishes never handed to a socket, and
    /// subscription changes, which still follow the publishes queued before
    /// them. A copy queued for a rate-limit resend is dropped: a publish that
    /// the socket never wrote comes back from the writer (`take_unwritten`), and
    /// one that it wrote is never sent again. The rate-limit streak and the
    /// probe schedule stay, since a reconnect does not refill a quota.
    pub(crate) fn drop_queue_connection(&mut self) {
        if let Some(timer) = self.queue.pause_timer.take() {
            timer.stop();
        }

        if let Some(timer) = self.queue.probe_timer.take() {
            timer.stop();
        }

        for abandoned in &mut self.queue.abandoned_interests {
            abandoned.clear();
        }

        self.queue.recent_interests.clear();
        self.queue.recent_publishes.clear();
        self.queue.first_sent_since_rate_limit_at = None;
        self.queue
            .publishes
            .retain(|publish| publish.connection().is_none());
    } // end method drop_queue_connection

    /// Returns publishes that a dropped socket never started to write to the
    /// front of the queue, in their order, ahead of everything queued after
    /// them.
    pub(crate) fn put_back(&mut self, mut publishes: Vec<Arc<QueuedPublish>>) {
        publishes.append(&mut self.queue.publishes);
        self.queue.publishes = publishes;
    } // end method put_back

    /// Syncs every held subscription to a new socket: messages, then presence,
    /// each in registration order. A sequence of zero puts it ahead of every
    /// queued publish, even one queued before the connection dropped, so the
    /// connection is a member of its segments again before its publishes join
    /// theirs (QUEUE-01).
    ///
    /// A new socket holds no subscription, so a waiting change that drops one
    /// is kept only behind a queued publish to its segment, which joins it. A
    /// publish never subscribes to presence.
    pub(crate) fn restore_interests(&mut self) {
        for kind in INTEREST_KINDS {
            let held = self.interests(kind).keys();
            let publishes = &self.queue.publishes;
            let pending = &mut self.queue.pending_interests[index(kind)];

            pending.retain(|segment_id, _| {
                let published = publishes
                    .iter()
                    .any(|publish| publish.segment_id == segment_id);

                kind == InterestKind::Message && published
            });

            for segment_id in held {
                pending.remove(&segment_id);
                pending.set(&segment_id, 0);
            }
        }

        self.drain();
    } // end method restore_interests

    /// Gives a command to the writer and records it as sent, as the reference
    /// does when it hands a command to the socket: a rate limit resends it, and
    /// it shows that commands flowed since the last limit.
    fn hand_off(&mut self, connection_id: u64, data: Bytes, publish: Option<Arc<QueuedPublish>>) {
        if let Some(publish) = &publish {
            self.record_sent_publish(publish);
        }

        if let Some(connection) = self.connection_mut(connection_id) {
            connection.hand_off(data, publish);
        }

        if self.queue.first_sent_since_rate_limit_at.is_none() {
            self.queue.first_sent_since_rate_limit_at = Some(Instant::now());
        }
    } // end method hand_off

    /// Gives the change a newer sequence, so that the sync follows every
    /// publish queued before it.
    fn mark_interest(&mut self, kind: InterestKind, segment_id: &str) {
        self.queue.sequence += 1;
        let sequence = self.queue.sequence;
        self.queue.pending_interests[index(kind)].set(segment_id, sequence);
    } // end method mark_interest

    fn requeue_recent(&mut self, now: Instant) {
        let recent_interests = std::mem::take(&mut self.queue.recent_interests);

        for sent in &recent_interests {
            if now.duration_since(sent.sent_at) <= RATE_LIMIT_SUSPECT_WINDOW {
                self.mark_interest(sent.kind, &sent.segment_id);
            }
        }

        let mut resent = Vec::new();

        for sent in &self.queue.recent_publishes {
            let mut progress = sent.publish.progress();

            if now.duration_since(sent.sent_at) <= RATE_LIMIT_SUSPECT_WINDOW
                && progress.resends < MAXIMUM_PUBLISH_RESENDS
            {
                progress.resends += 1;
                resent.push(Arc::clone(&sent.publish));
            }
        }

        self.put_back(resent);
    } // end method requeue_recent

    /// Drops recent publishes, and hands every subscription sent since the
    /// previous limit to the quota probe, however late the report: syncs are
    /// idempotent, so over-abandoning costs at most a redundant frame, while
    /// missing one loses the subscription.
    fn abandon_recent(&mut self) {
        for sent in &self.queue.recent_interests {
            self.queue.abandoned_interests[index(sent.kind)].set(&sent.segment_id, ());
        }

        let abandoned = self
            .queue
            .abandoned_interests
            .iter()
            .any(|abandoned| !abandoned.is_empty());

        if !abandoned || self.queue.probe_timer.is_some() {
            return;
        }

        let delay = QUOTA_PROBE_FIRST_DELAY
            .checked_mul(2_u32.saturating_pow(self.queue.probe_count))
            .map_or(QUOTA_PROBE_MAXIMUM_DELAY, |delay| {
                delay.min(QUOTA_PROBE_MAXIMUM_DELAY)
            });

        self.queue.probe_count += 1;
        self.queue.probe_timer = Some(self.start_timer(delay, ChannelShared::probe_elapsed));
    } // end method abandon_recent

    pub(crate) fn restore_abandoned(&mut self) {
        for kind in INTEREST_KINDS {
            let abandoned = std::mem::take(&mut self.queue.abandoned_interests[index(kind)]);

            for segment_id in abandoned.keys() {
                self.mark_interest(kind, &segment_id);
            }
        }
    } // end method restore_abandoned

    /// Restores abandoned subscriptions at once when commands went
    /// `quiet_span` without a rate limit after them: the quota is back.
    fn end_probing_if_quota_returned(&mut self, now: Instant, quiet_span: std::time::Duration) {
        let Some(first_sent) = self.queue.first_sent_since_rate_limit_at else {
            return;
        };

        if self.queue.probe_count == 0 || now.duration_since(first_sent) <= quiet_span {
            return;
        }

        if let Some(timer) = self.queue.probe_timer.take() {
            timer.stop();
        }

        self.queue.probe_count = 0;
        self.queue.rate_limit_streak = 0;
        self.restore_abandoned();
    } // end method end_probing_if_quota_returned

    /// Hands commands to the writer while it has room. The writer drains again
    /// whenever it finishes a write, so a full writer only delays commands.
    pub(crate) fn drain(&mut self) {
        let Some(connection_id) = self.current_connection else {
            return;
        };

        if self.queue.is_paused() {
            return;
        }

        self.end_probing_if_quota_returned(Instant::now(), RATE_LIMIT_SUSPECT_WINDOW);

        loop {
            if let Some((kind, segment_id)) = self.next_ready_interest() {
                if !self.hand_off_interest(connection_id, kind, &segment_id) {
                    return;
                }

                continue;
            }

            let Some(publish) = self.queue.publishes.first().cloned() else {
                return;
            };

            let has_room = self
                .connection(connection_id)
                .is_some_and(|connection| connection.has_room(publish.data.len()));

            if !has_room {
                return;
            }

            self.queue.publishes.remove(0);
            publish.set_connection(Some(connection_id));
            self.hand_off(connection_id, publish.data.clone(), Some(publish));
        }
    } // end method drain

    /// Finds the first subscription change with no earlier publish to its
    /// segment still queued: publishing joins the segment, so a change has to
    /// follow the publishes queued before it for the segment to end up as
    /// asked.
    fn next_ready_interest(&self) -> Option<(InterestKind, String)> {
        for kind in INTEREST_KINDS {
            for (segment_id, &sequence) in self.queue.pending_interests[index(kind)].iter() {
                let blocked =
                    self.queue.publishes.iter().any(|publish| {
                        publish.segment_id == segment_id && publish.sequence < sequence
                    });

                if !blocked {
                    return Some((kind, segment_id.to_owned()));
                }
            }
        }

        None
    } // end method next_ready_interest

    /// Hands the writer the command that brings the server in line with the
    /// segment's interest as it stands now. Reports `false` when the writer has
    /// no room.
    fn hand_off_interest(
        &mut self,
        connection_id: u64,
        kind: InterestKind,
        segment_id: &str,
    ) -> bool {
        if let Some(command) = self.interest_command(kind, segment_id) {
            // The segment was validated when its handle was made, so encoding
            // cannot fail.
            if let Ok(data) = encode_segment_command(command, segment_id) {
                let has_room = self
                    .connection(connection_id)
                    .is_some_and(|connection| connection.has_room(data.len()));

                if !has_room {
                    return false;
                }

                self.hand_off(connection_id, data, None);
                self.record_sent_interest(kind, segment_id);
            }
        }

        self.queue.pending_interests[index(kind)].remove(segment_id);

        true
    } // end method hand_off_interest

    /// Drops a cancelled publish from the resend record.
    pub(crate) fn forget_publish(&mut self, publish: &Arc<QueuedPublish>) {
        self.queue
            .recent_publishes
            .retain(|sent| !Arc::ptr_eq(&sent.publish, publish));
    } // end method forget_publish

    /// Drops the publishes that the server refused for their size from the
    /// resend record, so a later rate limit does not send, and bill, them
    /// again.
    pub(crate) fn forget_refused(&mut self, message_id: &str) {
        self.queue
            .recent_publishes
            .retain(|sent| sent.publish.message_id != message_id);
    } // end method forget_refused

    fn record_sent_interest(&mut self, kind: InterestKind, segment_id: &str) {
        let now = Instant::now();
        let recent = &mut self.queue.recent_interests;
        recent.retain(|sent| now.duration_since(sent.sent_at) <= RATE_LIMIT_SUSPECT_WINDOW);

        recent.push(SentInterest {
            kind,
            segment_id: segment_id.to_owned(),
            sent_at: now,
        });
    } // end method record_sent_interest

    fn record_sent_publish(&mut self, publish: &Arc<QueuedPublish>) {
        let now = Instant::now();
        let recent = &mut self.queue.recent_publishes;
        recent.retain(|sent| now.duration_since(sent.sent_at) <= RATE_LIMIT_SUSPECT_WINDOW);

        if recent.len() >= MAXIMUM_PENDING_COMMANDS {
            recent.drain(..=recent.len() - MAXIMUM_PENDING_COMMANDS);
        }

        recent.push(SentPublish {
            publish: Arc::clone(publish),
            sent_at: now,
        });
    } // end method record_sent_publish
} // end impl ChannelInner

impl ChannelShared {
    /// The pause after a rate limit is over: resend.
    fn pause_elapsed(self: &Arc<Self>, token: u64) {
        let mut inner = self.lock();

        if inner
            .queue
            .pause_timer
            .as_ref()
            .is_some_and(|timer| timer.token == token)
        {
            inner.queue.pause_timer = None;
            inner.drain();
        }
    } // end method pause_elapsed

    /// A quota probe is due: re-send the abandoned subscriptions.
    fn probe_elapsed(self: &Arc<Self>, token: u64) {
        let mut inner = self.lock();

        if inner
            .queue
            .probe_timer
            .as_ref()
            .is_some_and(|timer| timer.token == token)
        {
            inner.queue.probe_timer = None;
            inner.restore_abandoned();
            inner.drain();
        }
    } // end method probe_elapsed
} // end impl ChannelShared
