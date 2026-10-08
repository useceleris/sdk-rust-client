use tokio_tungstenite::tungstenite::Bytes;

use crate::constants::{MAXIMUM_COMMAND_BYTES, MAXIMUM_PRESENCE_PER_PAGE};
use crate::errors::{CelerisError, configuration_error, failure};
use crate::identifiers::identifier_rule;

const COMMAND_TOO_LARGE: &str = "Encoded command exceeds 2 MiB. That is the most the server accepts on any plan; send a smaller payload.";

/// The segment commands a client may send. Any other command, the server's
/// internal `NODE_*` commands included, cannot be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentCommand {
    Subscribe,
    Unsubscribe,
    PresenceSubscribe,
    PresenceUnsubscribe,
} // end enum SegmentCommand

impl SegmentCommand {
    pub(crate) fn name(self) -> &'static str {
        match self {
            SegmentCommand::Subscribe => "SUB",
            SegmentCommand::Unsubscribe => "UNSUB",
            SegmentCommand::PresenceSubscribe => "PRES_SUB",
            SegmentCommand::PresenceUnsubscribe => "PRES_UNSUB",
        }
    } // end method name
} // end impl SegmentCommand

/// Encodes `PUB`. A missing message id encodes as null; the channel always
/// supplies one (RESEND-01), so null is reachable only from tests.
pub(crate) fn encode_publish(
    segment_id: &str,
    message_id: Option<&str>,
    payload: &[u8],
) -> Result<Bytes, CelerisError> {
    let mut failures = Vec::new();

    if let Some(rule) = identifier_rule(segment_id) {
        failures.push(failure("segment_id", rule));
    }

    if let Some(rule) = message_id.and_then(identifier_rule) {
        failures.push(failure("message_id", rule));
    }

    if !failures.is_empty() {
        return Err(configuration_error("command", &failures));
    }

    let message_id_size =
        message_id.map_or("$-1\n".len(), |message_id| bulk_size(message_id.len()));
    let size =
        "@PUB\n".len() + bulk_size(segment_id.len()) + message_id_size + bulk_size(payload.len());

    if size > MAXIMUM_COMMAND_BYTES {
        return Err(CelerisError::Configuration(COMMAND_TOO_LARGE.to_owned()));
    }

    let mut command = Vec::with_capacity(size);
    command.extend_from_slice(b"@PUB\n");
    append_bulk(&mut command, segment_id.as_bytes());

    match message_id {
        Some(message_id) => append_bulk(&mut command, message_id.as_bytes()),
        None => command.extend_from_slice(b"$-1\n"),
    }

    append_bulk(&mut command, payload);

    Ok(Bytes::from(command))
} // end fn encode_publish

/// Encodes `SUB`, `UNSUB`, `PRES_SUB` or `PRES_UNSUB`.
pub(crate) fn encode_segment_command(
    command: SegmentCommand,
    segment_id: &str,
) -> Result<Bytes, CelerisError> {
    if let Some(rule) = identifier_rule(segment_id) {
        return Err(configuration_error(
            "command",
            &[failure("segment_id", rule)],
        ));
    }

    let name = command.name();
    let size = name.len() + 2 + bulk_size(segment_id.len());

    if size > MAXIMUM_COMMAND_BYTES {
        return Err(CelerisError::Configuration(COMMAND_TOO_LARGE.to_owned()));
    }

    let mut encoded = Vec::with_capacity(size);
    encoded.push(b'@');
    encoded.extend_from_slice(name.as_bytes());
    encoded.push(b'\n');
    append_bulk(&mut encoded, segment_id.as_bytes());

    Ok(Bytes::from(encoded))
} // end fn encode_segment_command

/// Encodes `PRES_LIST` with the query's request id (QUERY-01).
pub(crate) fn encode_presence_list(
    segment_id: &str,
    page: i32,
    per_page: i32,
    request_id: &str,
) -> Result<Bytes, CelerisError> {
    let mut failures = Vec::new();

    if let Some(rule) = identifier_rule(segment_id) {
        failures.push(failure("segment_id", rule));
    }

    if page < 1 {
        failures.push(failure("page", "Must be at least 1"));
    }

    if per_page < 1 {
        failures.push(failure("per_page", "Must be at least 1"));
    }

    if per_page > MAXIMUM_PRESENCE_PER_PAGE {
        failures.push(failure("per_page", "Must be at most 100"));
    }

    if let Some(rule) = identifier_rule(request_id) {
        failures.push(failure("request_id", rule));
    }

    if !failures.is_empty() {
        return Err(configuration_error("command", &failures));
    }

    let figures = format!(";{page}\n;{per_page}\n");
    let size = "@PRES_LIST\n".len()
        + bulk_size(segment_id.len())
        + figures.len()
        + bulk_size(request_id.len());

    if size > MAXIMUM_COMMAND_BYTES {
        return Err(CelerisError::Configuration(COMMAND_TOO_LARGE.to_owned()));
    }

    let mut command = Vec::with_capacity(size);
    command.extend_from_slice(b"@PRES_LIST\n");
    append_bulk(&mut command, segment_id.as_bytes());
    command.extend_from_slice(figures.as_bytes());
    append_bulk(&mut command, request_id.as_bytes());

    Ok(Bytes::from(command))
} // end fn encode_presence_list

/// The encoded size of a bulk string of the given length.
fn bulk_size(length: usize) -> usize {
    "$\n\n".len() + length.to_string().len() + length
} // end fn bulk_size

fn append_bulk(command: &mut Vec<u8>, value: &[u8]) {
    command.push(b'$');
    command.extend_from_slice(value.len().to_string().as_bytes());
    command.push(b'\n');
    command.extend_from_slice(value);
    command.push(b'\n');
} // end fn append_bulk
