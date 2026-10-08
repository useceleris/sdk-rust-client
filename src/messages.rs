use crate::errors::ServerError;
use crate::values::{PresenceEvent, PresencePage, ServerNotice};

/// One decoded server message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ServerMessage {
    Delivery(Delivery),
    Notice(ServerNotice),
    PresenceList(PresenceListResponse),
    PresenceNotify(PresenceEvent),
    Error(ServerError),
    Array(Vec<ServerMessage>),

    /// A command this version does not know. It is skipped, never surfaced
    /// (DECODE-01).
    Ignored,
} // end enum ServerMessage

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivery {
    pub(crate) token_reference: String,
    pub(crate) segment_id: String,

    /// `None` when the server sent null; such a delivery is dropped (REV-01).
    pub(crate) message_id: Option<String>,

    pub(crate) timestamp: i64,
    pub(crate) payload: Vec<u8>,
} // end struct Delivery

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresenceListResponse {
    pub(crate) request_id: String,
    pub(crate) page: PresencePage,
} // end struct PresenceListResponse
