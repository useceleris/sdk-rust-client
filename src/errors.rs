use std::fmt;

/// A failure the SDK detected, or an error frame the server sent.
///
/// The SDK's own variants are a closed set and carry a message that names what
/// failed and the rule or limit it broke. Such a message never carries input
/// values, credentials or bytes the server sent. Match on the variant, never on
/// the message.
///
/// There is no authentication variant: a handshake that the server refuses for
/// its credentials cannot be told apart from a network failure (DEV-02), so it
/// is reported as [`CelerisError::Transport`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CelerisError {
    /// Invalid input: options, identifiers, payload helpers, or credentials that
    /// a provider returned.
    Configuration(String),

    /// A connection attempt or a presence query ran past its deadline.
    Timeout(String),

    /// An operation stopped because the channel closed or a newer attempt
    /// started.
    Cancelled(String),

    /// A failed connection: the credential provider failed, the handshake was
    /// refused, or the socket broke.
    Transport(String),

    /// An operation needs a connection that the channel does not have.
    NotConnected(String),

    /// The publish queue is full, or sending is paused after a rate limit.
    /// Retry once it drains.
    Backpressure(String),

    /// A second connect or presence query while the first is still running.
    OperationInProgress(String),

    /// A publish may or may not have left the socket. It is never resent
    /// automatically.
    DeliveryUnknown(String),

    /// A server message could not be decoded. The message is dropped and the
    /// connection stays up (DECODE-01).
    Protocol {
        message: String,

        /// A field name that this crate chose.
        field: &'static str,

        /// A zero-based byte offset into the server message.
        offset: usize,
    },

    /// An error frame from the server, every field exactly as sent (ERR-01).
    Server(ServerError),
} // end enum CelerisError

impl fmt::Display for CelerisError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CelerisError::Configuration(message)
            | CelerisError::Timeout(message)
            | CelerisError::Cancelled(message)
            | CelerisError::Transport(message)
            | CelerisError::NotConnected(message)
            | CelerisError::Backpressure(message)
            | CelerisError::OperationInProgress(message)
            | CelerisError::DeliveryUnknown(message) => formatter.write_str(message),
            CelerisError::Protocol {
                message,
                field,
                offset,
            } => write!(formatter, "{message} Field: {field}, byte offset {offset}."),
            CelerisError::Server(server_error) => server_error.fmt(formatter),
        }
    } // end method fmt
} // end impl Display for CelerisError

impl std::error::Error for CelerisError {}

/// The kind of an error frame that the server sent. The set stays open: a type
/// that a newer server adds still reaches the caller as
/// [`ServerErrorType::Other`] (ERR-01).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ServerErrorType {
    /// The server could not parse a command.
    ParserError,

    /// The server failed to send.
    SendError,

    /// The token lacks access to what the command tried.
    PermissionDeniedError,

    /// A message limit was exceeded.
    RateLimitError,

    /// A publish payload exceeded the plan's cap, or a message exceeded the
    /// 2 MiB transport ceiling.
    MessageSizeLimitError,

    /// A server-side fault that the client did not cause.
    InternalError,

    /// A type that this version does not know, exactly as sent.
    Other(String),
} // end enum ServerErrorType

impl ServerErrorType {
    /// The type as the server wrote it.
    pub fn as_str(&self) -> &str {
        match self {
            ServerErrorType::ParserError => "ParserError",
            ServerErrorType::SendError => "SendError",
            ServerErrorType::PermissionDeniedError => "PermissionDeniedError",
            ServerErrorType::RateLimitError => "RateLimitError",
            ServerErrorType::MessageSizeLimitError => "MessageSizeLimitError",
            ServerErrorType::InternalError => "InternalError",
            ServerErrorType::Other(name) => name,
        }
    } // end method as_str

    pub(crate) fn from_wire(name: String) -> ServerErrorType {
        match name.as_str() {
            "ParserError" => ServerErrorType::ParserError,
            "SendError" => ServerErrorType::SendError,
            "PermissionDeniedError" => ServerErrorType::PermissionDeniedError,
            "RateLimitError" => ServerErrorType::RateLimitError,
            "MessageSizeLimitError" => ServerErrorType::MessageSizeLimitError,
            "InternalError" => ServerErrorType::InternalError,
            _ => ServerErrorType::Other(name),
        }
    } // end fn from_wire
} // end impl ServerErrorType

/// What an error frame's resource carries: whatever its type and sub type
/// define, such as the segment that a denial refers to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerErrorResource {
    Null,
    Text(String),
    Integer32(i32),
    Integer64(i64),
    Array(Vec<ServerErrorResource>),
} // end enum ServerErrorResource

/// An error frame from the server, every field exactly as sent (ERR-01). The
/// server keeps the connection open after it sends one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerError {
    pub error_type: ServerErrorType,

    /// The command that the error answers, such as `"SUB"` or `"PRES_LIST"`,
    /// or `None` when the server named none.
    pub sub_type: Option<String>,

    /// The server's own text, for people to read. Never parse it.
    pub message: String,

    pub resource: ServerErrorResource,
} // end struct ServerError

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.error_type.as_str(), self.message)
    } // end method fmt
} // end impl Display for ServerError

/// Names every field that failed and the rule it broke, never the value:
/// "Invalid client options. credential_provider: Required."
pub(crate) fn configuration_error(subject: &str, failures: &[String]) -> CelerisError {
    CelerisError::Configuration(format!("Invalid {subject}. {}", failures.join(" ")))
} // end fn configuration_error

/// Describes one failed field. An empty path describes the value as a whole.
pub(crate) fn failure(path: &str, rule: &str) -> String {
    if path.is_empty() {
        return format!("{rule}.");
    }

    format!("{path}: {rule}.")
} // end fn failure

pub(crate) fn protocol_error(message: &str, field: &'static str, offset: usize) -> CelerisError {
    CelerisError::Protocol {
        message: message.to_owned(),
        field,
        offset,
    }
} // end fn protocol_error
