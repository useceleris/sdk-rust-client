use std::sync::Arc;

use crate::channel::{ChannelInner, ChannelShared};
use crate::constants::PRESENCE_LIST_COMMAND;
use crate::errors::{
    CelerisError, ServerError, ServerErrorResource, ServerErrorType, protocol_error,
};
use crate::messages::{Delivery, PresenceListResponse, ServerMessage};
use crate::values::{MessageMetadata, PresenceEvent};

impl ChannelShared {
    /// Delivers one decoded message, entry by entry for a batch, and waits
    /// before each entry until no listener runs, so no message is routed while
    /// a listener runs: the read-ahead is one transport message, and there is
    /// no inbound queue (DEV-01). Reports `false` once the connection is no
    /// longer the channel's.
    pub(crate) async fn route(
        self: &Arc<Self>,
        connection_id: u64,
        message: Result<ServerMessage, CelerisError>,
    ) -> bool {
        let mut entries = Vec::new();

        match message {
            Ok(message) => flatten(message, &mut entries),
            Err(error) => entries.push(Err(error)),
        }

        for entry in entries {
            loop {
                let idle = self.idle.notified();
                tokio::pin!(idle);
                idle.as_mut().enable();

                {
                    let mut inner = self.lock();

                    if inner.current_connection != Some(connection_id) {
                        return false;
                    }

                    if inner.events.is_quiet() {
                        inner.route_entry(entry);
                        drop(inner);
                        self.dispatch_events();

                        break;
                    }
                }

                idle.await;
            }
        }

        true
    } // end method route
} // end impl ChannelShared

/// Unpacks batches in order and drops commands that this version skips. A
/// decoding failure is carried through routing, so it is reported in order
/// with everything else.
fn flatten(message: ServerMessage, entries: &mut Vec<Result<ServerMessage, CelerisError>>) {
    match message {
        ServerMessage::Array(messages) => {
            for message in messages {
                flatten(message, entries);
            }
        }
        ServerMessage::Ignored => {}
        message => entries.push(Ok(message)),
    }
} // end fn flatten

impl ChannelInner {
    fn route_entry(&mut self, entry: Result<ServerMessage, CelerisError>) {
        match entry {
            Ok(ServerMessage::Delivery(delivery)) => self.deliver(delivery),
            Ok(ServerMessage::Notice(notice)) => {
                self.queue_event(
                    |listeners| Some(&listeners.notices),
                    move |listener| listener(&notice),
                    true,
                );
            }
            Ok(ServerMessage::PresenceNotify(event)) => self.deliver_presence(event),
            Ok(ServerMessage::PresenceList(response)) => self.receive_presence_response(response),
            Ok(ServerMessage::Error(server_error)) => self.receive_server_error(server_error),
            Ok(ServerMessage::Array(_) | ServerMessage::Ignored) => {}
            Err(error) => self.queue_error(error),
        }
    } // end method route_entry

    fn deliver(&mut self, delivery: Delivery) {
        // Every delivery carries an id, the publisher's or one that the server
        // assigns (REV-01). One without leaves the message undeliverable, since
        // it cannot be deduplicated, so it is dropped and reported without
        // taking the connection down (DECODE-01).
        let Some(message_id) = delivery.message_id else {
            self.queue_error(protocol_error(
                "Server message is missing its identifier.",
                "MessageID",
                0,
            ));

            return;
        };

        // Ids are recorded before fan-out, even with no listeners.
        if !self.record_delivery(&message_id) {
            return;
        }

        let segment_id = delivery.segment_id;

        let delivered = Arc::new((
            delivery.payload,
            MessageMetadata {
                token_reference: delivery.token_reference,
                segment_id: segment_id.clone(),
                message_id,
                timestamp: delivery.timestamp,
            },
        ));

        let for_segment = Arc::clone(&delivered);

        // The segment's listeners first, then the channel's (MSG-02). Queued
        // even with no listeners: a set is read when its event's turn comes, so
        // a listener that another listener registers during this delivery
        // still receives the message, as in the reference.
        self.queue_event(
            move |listeners| listeners.segment_messages.get(&segment_id),
            move |listener| listener(&for_segment.0, &for_segment.1),
            true,
        );

        self.queue_event(
            |listeners| Some(&listeners.channel_messages),
            move |listener| listener(&delivered.0, &delivered.1),
            true,
        );
    } // end method deliver

    fn deliver_presence(&mut self, event: PresenceEvent) {
        let segment_id = event.segment_id.clone();

        self.queue_event(
            move |listeners| listeners.presence.get(&segment_id),
            move |listener| listener(&event),
            true,
        );
    } // end method deliver_presence

    /// Reports an error frame once, every field as sent, and leaves the
    /// connection up (ERR-01).
    fn receive_server_error(&mut self, server_error: ServerError) {
        // A presence query error names its query by request id and answers that
        // query alone. A stale id belongs to a query that was already rejected
        // and reported, so it is dropped (QUERY-01).
        if server_error.sub_type.as_deref() == Some(PRESENCE_LIST_COMMAND) {
            if let ServerErrorResource::Text(request_id) = &server_error.resource {
                let request_id = request_id.clone();
                self.settle_pending_presence(&request_id, Err(CelerisError::Server(server_error)));
            }

            return;
        }

        if server_error.error_type == ServerErrorType::RateLimitError {
            self.receive_rate_limit();
        }

        self.queue_error(CelerisError::Server(server_error));
    } // end method receive_server_error

    fn receive_presence_response(&mut self, response: PresenceListResponse) {
        // A response that carries any other request id answers a query that
        // already failed, so it is dropped; a pending query keeps waiting for
        // its own.
        self.settle_pending_presence(&response.request_id, Ok(response.page));
    } // end method receive_presence_response
} // end impl ChannelInner
