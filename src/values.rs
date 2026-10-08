use std::fmt;

/// Where a channel is in its lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChannelState {
    /// Created and never connected.
    Idle,

    /// The first connection attempt is running.
    Connecting,

    /// A WebSocket is established. Nothing more is implied.
    Connected,

    /// The connection dropped and is being recovered.
    Reconnecting,

    /// Connecting or recovery failed. Connect may be called again.
    Failed,

    /// Close is shutting the connection down.
    Closing,

    /// Terminal. Create a new channel to connect again.
    Closed,
} // end enum ChannelState

impl ChannelState {
    /// The state's name, as the reference implementation spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelState::Idle => "idle",
            ChannelState::Connecting => "connecting",
            ChannelState::Connected => "connected",
            ChannelState::Reconnecting => "reconnecting",
            ChannelState::Failed => "failed",
            ChannelState::Closing => "closing",
            ChannelState::Closed => "closed",
        }
    } // end method as_str
} // end impl ChannelState

impl fmt::Display for ChannelState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    } // end method fmt
} // end impl Display for ChannelState

/// Describes a delivery, beside its payload (MSG-01).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageMetadata {
    pub token_reference: String,
    pub segment_id: String,

    /// Always present: the publisher's, or one that the server assigned
    /// (REV-01).
    pub message_id: String,

    /// Unix milliseconds.
    pub timestamp: i64,
} // end struct MessageMetadata

/// A raw notice from the server: greetings, subscription acknowledgements and
/// refusals, as human-readable prose. It names no segment and no command, so
/// nothing may gate on its content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerNotice {
    /// Unix milliseconds.
    pub timestamp: i64,
    pub payload: Vec<u8>,
} // end struct ServerNotice

/// One page of a segment's presence, every figure exactly as the server sent
/// it. Past the last page `from` exceeds `to` and `connections` is empty; that
/// is not an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresencePage {
    pub segment_id: String,
    pub total: i32,
    pub per_page: i32,
    pub current_page: i32,
    pub from: i32,
    pub to: i32,
    pub connections: Vec<PresenceConnection>,
} // end struct PresencePage

/// One connection present in a segment. A token reference can hold several
/// connections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceConnection {
    pub token_reference: String,
    pub connection_id: String,

    /// Unix milliseconds.
    pub timestamp: i64,
} // end struct PresenceConnection

/// One connection that joined or left one segment (PRES-01).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceEvent {
    pub segment_id: String,
    pub token_reference: String,
    pub connection_id: String,

    /// `false` when the connection left.
    pub joined: bool,

    /// Unix milliseconds.
    pub timestamp: i64,
} // end struct PresenceEvent

/// Reports a reconnect. Replay is a bounded window, not a durable log, so gaps
/// and duplicates are always possible after one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryEvent {
    /// The zero-based retry that succeeded.
    pub retry_index: u32,

    /// Always `true`.
    pub possible_gaps: bool,

    /// Always `true`.
    pub possible_duplicates: bool,
} // end struct RecoveryEvent
