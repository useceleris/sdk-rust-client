//! Events are listener registrations that return a [`Registration`] to remove
//! the listener (DEV-01). Delivery is ordered, with no queue on the consumer's
//! side: a slow listener holds up the next event rather than letting events
//! pile up.
//!
//! Every event is queued under the channel's lock at the moment of the change
//! that causes it. One task at a time on Tokio's blocking thread pool delivers
//! the queue, and releases the lock around each listener. An event reaches the
//! listeners registered when its turn comes. Listeners therefore observe
//! events in the order that the state changed, never run under an SDK lock,
//! never hold up the runtime's worker threads, and may call back into the
//! channel.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use crate::channel::{ChannelInner, ChannelShared};
use crate::errors::CelerisError;
use crate::values::{ChannelState, MessageMetadata, PresenceEvent, RecoveryEvent, ServerNotice};

pub(crate) type MessageListener = dyn Fn(&[u8], &MessageMetadata) + Send + Sync;

pub(crate) type PresenceListener = dyn Fn(&PresenceEvent) + Send + Sync;

pub(crate) type StateListener = dyn Fn(ChannelState) + Send + Sync;

pub(crate) type RecoveryListener = dyn Fn(&RecoveryEvent) + Send + Sync;

pub(crate) type NoticeListener = dyn Fn(&ServerNotice) + Send + Sync;

pub(crate) type ErrorListener = dyn Fn(&CelerisError) + Send + Sync;

/// One delivery, run without the lock.
type Delivery = Box<dyn FnOnce() + Send>;

/// One queued event. The drainer prepares it under the lock: it takes the
/// snapshot of the listeners and returns the delivery to run without the lock.
pub(crate) type Event = Box<dyn FnOnce(&ChannelInner) -> Delivery + Send>;

/// The queued events and whether a task is delivering them. Guarded by the
/// channel's lock.
#[derive(Default)]
pub(crate) struct EventQueue {
    events: VecDeque<Event>,
    draining: bool,
} // end struct EventQueue

impl EventQueue {
    /// No event is queued or being delivered.
    pub(crate) fn is_quiet(&self) -> bool {
        !self.draining && self.events.is_empty()
    } // end method is_quiet
} // end impl EventQueue

/// Removes one listener. Call [`Registration::remove`] to remove it; calling it
/// again does nothing.
///
/// Dropping a `Registration` keeps the listener registered, so a listener that
/// stays for the life of the channel needs no binding.
#[derive(Debug)]
pub struct Registration {
    channel: Weak<ChannelShared>,
    location: ListenerLocation,
    identifier: u64,
    removed: Arc<AtomicBool>,
} // end struct Registration

impl Registration {
    /// Removes the listener at once. The other listeners and the subscriptions
    /// do not change, and nothing is sent to the server.
    pub fn remove(&self) {
        // Set first, so that a delivery in progress skips the listener.
        if self.removed.swap(true, Ordering::SeqCst) {
            return;
        }

        if let Some(channel) = self.channel.upgrade() {
            channel
                .lock()
                .listeners
                .remove(&self.location, self.identifier);
        }
    } // end method remove
} // end impl Registration

/// Registers channel-wide listeners. Obtain one with
/// [`Channel::events`](crate::Channel::events).
#[derive(Clone)]
pub struct ChannelEventHandler {
    pub(crate) channel: Arc<ChannelShared>,
} // end struct ChannelEventHandler

impl fmt::Debug for ChannelEventHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ChannelEventHandler")
    } // end method fmt
} // end impl Debug for ChannelEventHandler

impl ChannelEventHandler {
    /// Registers a listener for every state transition.
    pub fn on_state_change(
        &self,
        listener: impl Fn(ChannelState) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<StateListener> = Arc::new(listener);

        self.channel.register(
            ListenerLocation::State,
            |listeners| &mut listeners.states,
            listener,
        )
    } // end method on_state_change

    /// Registers a listener for every successful reconnect.
    pub fn on_recovery(
        &self,
        listener: impl Fn(&RecoveryEvent) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<RecoveryListener> = Arc::new(listener);

        self.channel.register(
            ListenerLocation::Recovery,
            |listeners| &mut listeners.recoveries,
            listener,
        )
    } // end method on_recovery

    /// Registers a listener for the server's raw notices.
    pub fn on_notice(
        &self,
        listener: impl Fn(&ServerNotice) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<NoticeListener> = Arc::new(listener);

        self.channel.register(
            ListenerLocation::Notice,
            |listeners| &mut listeners.notices,
            listener,
        )
    } // end method on_notice

    /// Registers a listener for failures that no caller waits for: a
    /// [`CelerisError::Server`] that the server sent, or an SDK error such as
    /// an undecodable message, a failed reconnect, or a listener that panicked.
    pub fn on_error(
        &self,
        listener: impl Fn(&CelerisError) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<ErrorListener> = Arc::new(listener);

        self.channel.register(
            ListenerLocation::Error,
            |listeners| &mut listeners.errors,
            listener,
        )
    } // end method on_error

    /// Registers a listener for every delivery from any segment that the
    /// connection is a member of, also when that segment has no listeners of
    /// its own. For each delivery it runs after the segment's listeners
    /// (MSG-02).
    ///
    /// Removing it removes only this listener: the other listeners and the
    /// subscriptions do not change, and nothing is sent to the server.
    pub fn on_message(
        &self,
        listener: impl Fn(&[u8], &MessageMetadata) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<MessageListener> = Arc::new(listener);

        self.channel.register(
            ListenerLocation::ChannelMessage,
            |listeners| &mut listeners.channel_messages,
            listener,
        )
    } // end method on_message
} // end impl ChannelEventHandler

/// Where a listener is registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ListenerLocation {
    State,
    Recovery,
    Notice,
    Error,
    ChannelMessage,
    SegmentMessage(String),
    Presence(String),
} // end enum ListenerLocation

struct ListenerEntry<Listener: ?Sized> {
    identifier: u64,

    // Read without the lock while events are delivered, so an entry removed
    // before its turn is skipped.
    removed: Arc<AtomicBool>,

    listener: Arc<Listener>,
} // end struct ListenerEntry

pub(crate) struct ListenerSet<Listener: ?Sized> {
    entries: Vec<Arc<ListenerEntry<Listener>>>,
} // end struct ListenerSet

impl<Listener: ?Sized> Default for ListenerSet<Listener> {
    fn default() -> ListenerSet<Listener> {
        ListenerSet {
            entries: Vec::new(),
        }
    } // end fn default
} // end impl Default for ListenerSet

impl<Listener: ?Sized> ListenerSet<Listener> {
    fn remove(&mut self, identifier: u64) {
        self.entries.retain(|entry| entry.identifier != identifier);
    } // end method remove

    fn snapshot(&self) -> Vec<Arc<ListenerEntry<Listener>>> {
        self.entries.clone()
    } // end method snapshot
} // end impl ListenerSet

/// Every listener of a channel. Guarded by the channel's lock.
#[derive(Default)]
pub(crate) struct Listeners {
    pub(crate) segment_messages: HashMap<String, ListenerSet<MessageListener>>,
    pub(crate) presence: HashMap<String, ListenerSet<PresenceListener>>,
    pub(crate) channel_messages: ListenerSet<MessageListener>,
    pub(crate) states: ListenerSet<StateListener>,
    pub(crate) recoveries: ListenerSet<RecoveryListener>,
    pub(crate) notices: ListenerSet<NoticeListener>,
    pub(crate) errors: ListenerSet<ErrorListener>,
    last_identifier: u64,
} // end struct Listeners

impl Listeners {
    fn remove(&mut self, location: &ListenerLocation, identifier: u64) {
        match location {
            ListenerLocation::State => self.states.remove(identifier),
            ListenerLocation::Recovery => self.recoveries.remove(identifier),
            ListenerLocation::Notice => self.notices.remove(identifier),
            ListenerLocation::Error => self.errors.remove(identifier),
            ListenerLocation::ChannelMessage => self.channel_messages.remove(identifier),
            ListenerLocation::SegmentMessage(segment_id) => {
                if let Some(set) = self.segment_messages.get_mut(segment_id) {
                    set.remove(identifier);
                }
            }
            ListenerLocation::Presence(segment_id) => {
                if let Some(set) = self.presence.get_mut(segment_id) {
                    set.remove(identifier);
                }
            }
        }
    } // end method remove
} // end impl Listeners

impl ChannelShared {
    /// Adds a listener to the set that `choose` picks, which must be the set
    /// that `location` names.
    pub(crate) fn register<Listener: ?Sized>(
        self: &Arc<Self>,
        location: ListenerLocation,
        choose: impl FnOnce(&mut Listeners) -> &mut ListenerSet<Listener>,
        listener: Arc<Listener>,
    ) -> Registration {
        let removed = Arc::new(AtomicBool::new(false));
        let mut inner = self.lock();
        inner.listeners.last_identifier += 1;
        let identifier = inner.listeners.last_identifier;

        choose(&mut inner.listeners)
            .entries
            .push(Arc::new(ListenerEntry {
                identifier,
                removed: Arc::clone(&removed),
                listener,
            }));

        Registration {
            channel: Arc::downgrade(self),
            location,
            identifier,
            removed,
        }
    } // end method register

    /// Delivers queued events in order until none remain. Only one task
    /// delivers at a time; a call while one delivers returns at once, and the
    /// events it queued are delivered by the one already delivering.
    pub(crate) fn dispatch_events(self: &Arc<Self>) {
        let mut inner = self.lock();

        if inner.events.draining || inner.events.events.is_empty() {
            return;
        }

        let Some(runtime) = inner.runtime.clone() else {
            return;
        };

        inner.events.draining = true;
        drop(inner);

        let channel = Arc::clone(self);
        runtime.spawn_blocking(move || channel.drain_events());
    } // end method dispatch_events

    fn drain_events(&self) {
        loop {
            let delivery = {
                let mut inner = self.lock();

                let Some(event) = inner.events.events.pop_front() else {
                    inner.events.draining = false;
                    drop(inner);
                    self.idle.notify_waiters();

                    return;
                };

                event(&inner)
            };

            delivery();
        }
    } // end method drain_events

    /// Waits until no event is queued or being delivered.
    #[cfg(test)]
    pub(crate) async fn quiesce(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();

            if self.lock().events.is_quiet() {
                return;
            }

            idle.await;
        }
    } // end method quiesce
} // end impl ChannelShared

impl ChannelInner {
    /// Queues delivery of one event to the listeners that `choose` picks when
    /// its turn comes, as the reference dispatches to the listeners registered
    /// at dispatch. A listener that panics is reported through the error
    /// listeners; an error listener that panics is not, since that would
    /// report into the dispatch that failed.
    pub(crate) fn queue_event<Listener: ?Sized + Send + Sync + 'static>(
        &mut self,
        choose: impl FnOnce(&Listeners) -> Option<&ListenerSet<Listener>> + Send + 'static,
        invoke: impl Fn(&Listener) + Send + 'static,
        report_panics: bool,
    ) {
        self.events
            .events
            .push_back(Box::new(move |inner: &ChannelInner| {
                let entries = choose(&inner.listeners)
                    .map(ListenerSet::snapshot)
                    .unwrap_or_default();
                let channel = inner.this.clone();

                Box::new(move || deliver_entries(&channel, &entries, &invoke, report_panics))
                    as Delivery
            }));
    } // end method queue_event

    pub(crate) fn queue_state_change(&mut self, state: ChannelState) {
        self.state = state;
        self.queue_event(
            |listeners| Some(&listeners.states),
            move |listener| listener(state),
            true,
        );
    } // end method queue_state_change

    pub(crate) fn queue_error(&mut self, error: CelerisError) {
        self.queue_event(
            |listeners| Some(&listeners.errors),
            move |listener| listener(&error),
            false,
        );
    } // end method queue_error
} // end impl ChannelInner

fn deliver_entries<Listener: ?Sized>(
    channel: &Weak<ChannelShared>,
    entries: &[Arc<ListenerEntry<Listener>>],
    invoke: &impl Fn(&Listener),
    report_panics: bool,
) {
    for entry in entries {
        if entry.removed.load(Ordering::SeqCst) {
            continue;
        }

        let panicked = catch_unwind(AssertUnwindSafe(|| invoke(&entry.listener))).is_err();

        if panicked && report_panics {
            report_listener_panic(channel);
        }
    }
} // end fn deliver_entries

/// Reports a listener's panic next, ahead of the events already queued, as the
/// reference reports it before dispatch moves on.
fn report_listener_panic(channel: &Weak<ChannelShared>) {
    let Some(channel) = channel.upgrade() else {
        return;
    };

    let failure = CelerisError::Transport(
        "A listener callback panicked; the channel recovered and kept running.".to_owned(),
    );
    let mut inner = channel.lock();
    inner.queue_error(failure);

    // queue_error appends; the report belongs at the front.
    if let Some(report) = inner.events.events.pop_back() {
        inner.events.events.push_front(report);
    }
} // end fn report_listener_panic
