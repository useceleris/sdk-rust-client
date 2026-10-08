use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::channel::ChannelShared;
use crate::command_queue::InterestKind;
use crate::errors::CelerisError;
use crate::events::{ListenerLocation, MessageListener, PresenceListener, Registration};
use crate::random::generate_message_id;
use crate::values::{MessageMetadata, PresenceEvent, PresencePage};

/// A handle for one segment of a channel. It holds only its id:
/// subscriptions, listeners and the connection belong to the channel, so any
/// number of handles for one segment share them. Obtain one with
/// [`Channel::segment`](crate::Channel::segment) or
/// [`Channel::default_segment`](crate::Channel::default_segment).
#[derive(Clone)]
pub struct Segment {
    channel: Arc<ChannelShared>,
    segment_id: String,
} // end struct Segment

impl fmt::Debug for Segment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Segment")
            .field("segment_id", &self.segment_id)
            .finish()
    } // end method fmt
} // end impl Debug for Segment

impl Segment {
    pub(crate) fn new(channel: Arc<ChannelShared>, segment_id: &str) -> Segment {
        Segment {
            channel,
            segment_id: segment_id.to_owned(),
        }
    } // end fn new

    /// The segment's id.
    pub fn id(&self) -> &str {
        &self.segment_id
    } // end method id

    /// Registers interest in the segment's messages. The first interest in a
    /// segment subscribes it, and it stays subscribed until every interest is
    /// cancelled; subscriptions are restored after every reconnect. The
    /// default segment is joined automatically and never subscribed or
    /// unsubscribed.
    ///
    /// No acknowledgement exists: a denial arrives later through
    /// [`ChannelEventHandler::on_error`](crate::ChannelEventHandler::on_error).
    /// It fails with [`CelerisError::NotConnected`] once the channel is closing
    /// or closed.
    pub fn subscribe(&self) -> Result<Subscription, CelerisError> {
        self.channel
            .subscribe(InterestKind::Message, &self.segment_id)
    } // end method subscribe

    /// Registers interest in the segment's presence joins and leaves. Watching
    /// presence is not membership: it delivers no messages, and the watcher is
    /// not itself announced or listed.
    pub fn subscribe_presence(&self) -> Result<Subscription, CelerisError> {
        self.channel
            .subscribe(InterestKind::Presence, &self.segment_id)
    } // end method subscribe_presence

    /// Registers a listener for the segment's deliveries. Each message id is
    /// delivered once within a bounded window (REV-01).
    pub fn on_message(
        &self,
        listener: impl Fn(&[u8], &MessageMetadata) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<MessageListener> = Arc::new(listener);
        let segment_id = self.segment_id.clone();

        self.channel.register(
            ListenerLocation::SegmentMessage(self.segment_id.clone()),
            move |listeners| listeners.segment_messages.entry(segment_id).or_default(),
            listener,
        )
    } // end method on_message

    /// Registers a listener for joins and leaves. Events arrive only while a
    /// presence subscription is held: the server sends them to presence
    /// subscribers alone (PRES-01).
    pub fn on_presence(
        &self,
        listener: impl Fn(&PresenceEvent) + Send + Sync + 'static,
    ) -> Registration {
        let listener: Arc<PresenceListener> = Arc::new(listener);
        let segment_id = self.segment_id.clone();

        self.channel.register(
            ListenerLocation::Presence(self.segment_id.clone()),
            move |listeners| listeners.presence.entry(segment_id).or_default(),
            listener,
        )
    } // end method on_presence

    /// Sends `payload` to the segment with a generated message id. It returns
    /// once the socket accepted the bytes. That is not receipt, delivery or
    /// durability: the protocol has no acknowledgements.
    ///
    /// While the channel reconnects, the publish waits in the queue and is sent
    /// after the reconnect, once the restored subscriptions have gone out. It
    /// then fails with the error that
    /// [`on_error`](crate::ChannelEventHandler::on_error) reports if recovery
    /// stops in [`Failed`](crate::ChannelState::Failed), and with
    /// [`CelerisError::Cancelled`] if the channel closes.
    ///
    /// It fails with [`CelerisError::NotConnected`] before the first connect,
    /// while connecting, and once failed or closed; with
    /// [`CelerisError::Backpressure`] when the publish queue (64 by default) is
    /// already full; and with [`CelerisError::DeliveryUnknown`] when the socket
    /// failed during the write, which is never resent. A publish over the
    /// plan's payload cap still succeeds here and is refused afterwards with a
    /// `MessageSizeLimitError` through `on_error`.
    ///
    /// Dropping the returned future takes the publish back if the socket has
    /// not started to write it.
    pub async fn publish(&self, payload: impl AsRef<[u8]>) -> Result<(), CelerisError> {
        let message_id = generate_message_id();

        self.channel
            .publish(&self.segment_id, payload.as_ref(), &message_id)
            .await
    } // end method publish

    /// [`Segment::publish`] with your own message id. Receivers drop a repeated
    /// id within their deduplication window, so reuse an id only to resend the
    /// same message.
    pub async fn publish_with_message_id(
        &self,
        payload: impl AsRef<[u8]>,
        message_id: &str,
    ) -> Result<(), CelerisError> {
        self.channel
            .publish(&self.segment_id, payload.as_ref(), message_id)
            .await
    } // end method publish_with_message_id

    /// One page of the segment's presence. One query may be in flight per
    /// channel; it fails with [`CelerisError::OperationInProgress`] while
    /// another is pending, and with [`CelerisError::Timeout`] after the
    /// presence query timeout. `page` starts at 1, and `per_page` is 1 to 100.
    ///
    /// Dropping the returned future frees the query slot.
    pub async fn presence_list(
        &self,
        page: i32,
        per_page: i32,
    ) -> Result<PresencePage, CelerisError> {
        self.channel
            .query_presence(&self.segment_id, page, per_page)
            .await
    } // end method presence_list
} // end impl Segment

/// One registered interest. Cancel it when done; dropping it keeps the
/// interest.
pub struct Subscription {
    channel: Arc<ChannelShared>,
    kind: InterestKind,
    segment_id: String,
    cancelled: AtomicBool,
} // end struct Subscription

impl fmt::Debug for Subscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Subscription")
            .field("segment_id", &self.segment_id)
            .field("presence", &(self.kind == InterestKind::Presence))
            .finish()
    } // end method fmt
} // end impl Debug for Subscription

impl Subscription {
    pub(crate) fn new(
        channel: Arc<ChannelShared>,
        kind: InterestKind,
        segment_id: &str,
    ) -> Subscription {
        Subscription {
            channel,
            kind,
            segment_id: segment_id.to_owned(),
            cancelled: AtomicBool::new(false),
        }
    } // end fn new

    /// Releases the interest at once. Cancelling the last message interest
    /// unsubscribes the segment, and cancelling the last presence interest
    /// stops its presence events; neither affects the other. Calling `cancel`
    /// again does nothing.
    pub fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }

        self.channel
            .lock()
            .release_interest(self.kind, &self.segment_id);
    } // end method cancel
} // end impl Subscription
