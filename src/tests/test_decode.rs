//! Decoder golden vectors and failures (WIRE-01 to WIRE-06, DECODE-01, ERR-01,
//! LIMIT-01). Every vector is written by hand, from the reference's codec
//! vectors, and every failure names its exact reason, field and offset.

use crate::decode::decode_server_message;
use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::messages::{Delivery, PresenceListResponse, ServerMessage};
use crate::tests::support;
use crate::values::{PresenceConnection, PresenceEvent, PresencePage, ServerNotice};

const INVALID_UTF8: [&[u8]; 5] = [
    &[0x80],
    &[0xc0, 0xaf],
    &[0xed, 0xa0, 0x80],
    &[0xf0, 0x9f, 0x98],
    &[0xf4, 0x90, 0x80, 0x80],
];

/// Every reason the decoder can give, with each figure as `N`.
const DECODER_REASONS: [&str; 29] = [
    "Missing field marker.",
    "Unexpected server message marker.",
    "Trailing data after server message.",
    "Server message has more than N fragments.",
    "Line exceeds its N-byte limit.",
    "Unterminated line.",
    "Invalid UTF-N text.",
    "Expected decimal digits.",
    "Integer is outside -N to N.",
    "Expected IntegerN marker.",
    "Expected simple or bulk byte marker.",
    "Invalid bulk byte length.",
    "Bulk payload exceeds remaining message bytes.",
    "Missing bulk byte terminator.",
    "Identifier cannot be null.",
    "Identifier must be nonempty and CR/LF-free.",
    "Payload cannot be null.",
    "Arrays are nested deeper than N levels.",
    "Expected array marker.",
    "Array length cannot be negative.",
    "Array length exceeds the N-fragment budget.",
    "Presence connection must contain three fields.",
    "Invalid error header.",
    "Expected simple string marker.",
    "Sub type must be a simple string or null.",
    "Invalid error name.",
    "Unexpected resource marker.",
    "Unknown command inside array has ambiguous boundaries.",
    "Presence event must be N or N.",
];

fn join(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
} // end fn join

fn located(message: &str, field: &'static str, offset: usize) -> CelerisError {
    CelerisError::Protocol {
        message: message.to_owned(),
        field,
        offset,
    }
} // end fn located

fn notice(timestamp: i64, payload: &[u8]) -> ServerMessage {
    ServerMessage::Notice(ServerNotice {
        timestamp,
        payload: payload.to_vec(),
    })
} // end fn notice

fn delivery(
    token_reference: &str,
    segment_id: &str,
    message_id: Option<&str>,
    timestamp: i64,
    payload: &[u8],
) -> ServerMessage {
    ServerMessage::Delivery(Delivery {
        token_reference: token_reference.to_owned(),
        segment_id: segment_id.to_owned(),
        message_id: message_id.map(str::to_owned),
        timestamp,
        payload: payload.to_vec(),
    })
} // end fn delivery

fn presence_list(
    request_id: &str,
    figures: [i32; 5],
    connections: Vec<PresenceConnection>,
) -> ServerMessage {
    let [total, per_page, current_page, from, to] = figures;

    ServerMessage::PresenceList(PresenceListResponse {
        request_id: request_id.to_owned(),
        page: PresencePage {
            segment_id: "chat".to_owned(),
            total,
            per_page,
            current_page,
            from,
            to,
            connections,
        },
    })
} // end fn presence_list

fn presence_notify(joined: bool, timestamp: i64) -> ServerMessage {
    ServerMessage::PresenceNotify(PresenceEvent {
        segment_id: "chat".to_owned(),
        token_reference: "user".to_owned(),
        connection_id: "connection".to_owned(),
        joined,
        timestamp,
    })
} // end fn presence_notify

fn server_error(
    error_type: ServerErrorType,
    sub_type: Option<&str>,
    message: &str,
    resource: ServerErrorResource,
) -> ServerMessage {
    ServerMessage::Error(ServerError {
        error_type,
        sub_type: sub_type.map(str::to_owned),
        message: message.to_owned(),
        resource,
    })
} // end fn server_error

fn text(value: &str) -> ServerErrorResource {
    ServerErrorResource::Text(value.to_owned())
} // end fn text

fn decoding_vectors() -> Vec<(String, Vec<u8>, ServerMessage)> {
    let mut vectors: Vec<(String, Vec<u8>, ServerMessage)> = vec![
        (
            "peer null ID and zero timestamp".to_owned(),
            b"@MSG\n+user\n+chat\n$-1\n:0\n$0\n\n".to_vec(),
            delivery("user", "chat", None, 0, b""),
        ),
        (
            "peer Unicode and max timestamp".to_owned(),
            join(&[
                "@MSG\n+用戶\n+c:é\n+識\n:9223372036854775807\n$5\n".as_bytes(),
                &[0, 255, 13, 10, 64, 10],
            ]),
            delivery("用戶", "c:é", Some("識"), i64::MAX, &[0, 255, 13, 10, 64]),
        ),
        (
            "CRLF and min timestamp".to_owned(),
            b"@SERVER_MSG\r\n:-9223372036854775808\r\n$2\r\nok\r\n".to_vec(),
            notice(i64::MIN, b"ok"),
        ),
        (
            "raw notice".to_owned(),
            b"@SERVER_MSG\n:1\n$6\njoined\n".to_vec(),
            notice(1, b"joined"),
        ),
        (
            "simple payload and bulk identifiers".to_owned(),
            b"@MSG\n$1\nu\n$1\ns\n$1\nm\n:-1\n+data\n".to_vec(),
            delivery("u", "s", Some("m"), -1, b"data"),
        ),
        (
            "preserve BOM in text".to_owned(),
            "@MSG\n+\u{FEFF}u\n+s\n$-1\n:1\n$0\n\n".as_bytes().to_vec(),
            delivery("\u{FEFF}u", "s", None, 1, b""),
        ),
        (
            "empty array".to_owned(),
            b"*0\n".to_vec(),
            ServerMessage::Array(vec![]),
        ),
        (
            "nested arrays".to_owned(),
            b"*2\n*0\n*1\n@SERVER_MSG\n:1\n$0\n\n".to_vec(),
            ServerMessage::Array(vec![
                ServerMessage::Array(vec![]),
                ServerMessage::Array(vec![notice(1, b"")]),
            ]),
        ),
        (
            "presence connections".to_owned(),
            b"@PRES_LIST_RESPONSE\n+chat\n$1\n7\n;1\n;25\n;1\n;1\n;1\n*1\n*3\n+user\n+connection\n:123\n"
                .to_vec(),
            presence_list(
                "7",
                [1, 25, 1, 1, 1],
                vec![PresenceConnection {
                    token_reference: "user".to_owned(),
                    connection_id: "connection".to_owned(),
                    timestamp: 123,
                }],
            ),
        ),
        (
            "presence empty".to_owned(),
            b"@PRES_LIST_RESPONSE\n+chat\n$1\n8\n;0\n;25\n;1\n;0\n;0\n*0\n".to_vec(),
            presence_list("8", [0, 25, 1, 0, 0], vec![]),
        ),
        (
            "presence notification join".to_owned(),
            b"@PRES_NOTIFY\n+chat\n+user\n+connection\n;1\n:123\n".to_vec(),
            presence_notify(true, 123),
        ),
        (
            "presence notification leave".to_owned(),
            b"@PRES_NOTIFY\n+chat\n+user\n+connection\n;0\n:124\n".to_vec(),
            presence_notify(false, 124),
        ),
        (
            "unknown command ignored".to_owned(),
            b"@FUTURE_COMMAND\n+a\n:1\n".to_vec(),
            ServerMessage::Ignored,
        ),
        (
            "unknown command ignored in tail position".to_owned(),
            b"*2\n@SERVER_MSG\n:1\n$0\n\n@FUTURE_COMMAND\n+a\n".to_vec(),
            ServerMessage::Array(vec![notice(1, b""), ServerMessage::Ignored]),
        ),
        (
            "internal node command ignored".to_owned(),
            b"@NODE_PUB\n+node-1\n$4\nbody\n".to_vec(),
            ServerMessage::Ignored,
        ),
        (
            "internal node command ignored in tail position".to_owned(),
            b"*2\n@SERVER_MSG\n:1\n$0\n\n@NODE_PUB\n+node-1\n".to_vec(),
            ServerMessage::Array(vec![notice(1, b""), ServerMessage::Ignored]),
        ),
        (
            "any NODE_ command ignored".to_owned(),
            b"@NODE_FUTURE\n+a\n".to_vec(),
            ServerMessage::Ignored,
        ),
        (
            "presence past last page".to_owned(),
            b"@PRES_LIST_RESPONSE\n+chat\n$1\n9\n;1\n;25\n;2\n;26\n;1\n*0\n".to_vec(),
            presence_list("9", [1, 25, 2, 26, 1], vec![]),
        ),
        (
            "error without sub type or resource".to_owned(),
            b"-Err\n+RateLimitError\n$-1\n$4\nslow\n$-1\n".to_vec(),
            server_error(
                ServerErrorType::RateLimitError,
                None,
                "slow",
                ServerErrorResource::Null,
            ),
        ),
        (
            "error message containing frame text".to_owned(),
            b"-Err\n+PermissionDeniedError\n+SUB\n$14\ntext\n-Err\nmore\n$4\nroom\n".to_vec(),
            server_error(
                ServerErrorType::PermissionDeniedError,
                Some("SUB"),
                "text\n-Err\nmore",
                text("room"),
            ),
        ),
        (
            "presence query error carrying its request id".to_owned(),
            b"-Err\n+InternalError\n+PRES_LIST\n$27\nError getting presence data\n$1\n3\n".to_vec(),
            server_error(
                ServerErrorType::InternalError,
                Some("PRES_LIST"),
                "Error getting presence data",
                text("3"),
            ),
        ),
        (
            "error resource of every shape".to_owned(),
            b"-Err\n+FutureError\n+PUB\n$1\nx\n*5\n+a\n:-5\n;7\n$-1\n*1\n$1\nb\n".to_vec(),
            server_error(
                ServerErrorType::Other("FutureError".to_owned()),
                Some("PUB"),
                "x",
                ServerErrorResource::Array(vec![
                    text("a"),
                    ServerErrorResource::Integer64(-5),
                    ServerErrorResource::Integer32(7),
                    ServerErrorResource::Null,
                    ServerErrorResource::Array(vec![text("b")]),
                ]),
            ),
        ),
        (
            "batched errors before other messages".to_owned(),
            b"*3\n-Err\n+PermissionDeniedError\n+PRES_SUB\n$2\nno\n$4\nroom\n-Err\n+PermissionDeniedError\n+PRES_LIST\n$2\nno\n$1\n4\n@SERVER_MSG\n:1\n$2\nok\n"
                .to_vec(),
            ServerMessage::Array(vec![
                server_error(
                    ServerErrorType::PermissionDeniedError,
                    Some("PRES_SUB"),
                    "no",
                    text("room"),
                ),
                server_error(
                    ServerErrorType::PermissionDeniedError,
                    Some("PRES_LIST"),
                    "no",
                    text("4"),
                ),
                notice(1, b"ok"),
            ]),
        ),
    ];

    // Invalid text encodings are still valid opaque binary payloads.
    for invalid in INVALID_UTF8 {
        vectors.push((
            format!("opaque non-UTF8 payload {invalid:?}"),
            join(&[
                format!("@SERVER_MSG\n:1\n${}\n", invalid.len()).as_bytes(),
                invalid,
                b"\n",
            ]),
            notice(1, invalid),
        ));
    }

    vectors
} // end fn decoding_vectors

/// The reference's malformed vectors, each with the exact failure.
fn malformed_vectors() -> Vec<(Vec<u8>, CelerisError)> {
    let deeper = "Arrays are nested deeper than 32 levels.";
    let digits = "Expected decimal digits.";
    let identifier = "Identifier must be nonempty and CR/LF-free.";
    let int32_range = "Integer is outside -2147483648 to 2147483647.";
    let int64_range = "Integer is outside -9223372036854775808 to 9223372036854775807.";
    let invalid_name = "Invalid error name.";
    let missing = "Missing field marker.";
    let not_simple = "Expected simple string marker.";
    let sub_type = "Sub type must be a simple string or null.";
    let terminator = "Missing bulk byte terminator.";
    let unexpected = "Unexpected server message marker.";
    let unknown = "Unknown command inside array has ambiguous boundaries.";

    let texts: Vec<(String, CelerisError)> = vec![
        ("".to_owned(), located(missing, "Message", 0)),
        (
            "*0\ntrailing".to_owned(),
            located("Trailing data after server message.", "Message", 3),
        ),
        ("*1\n".to_owned(), located(missing, "Message", 3)),
        (
            "*-1\n".to_owned(),
            located("Array length cannot be negative.", "Messages", 0),
        ),
        (
            "*4096\n".to_owned(),
            located(
                "Array length exceeds the 4096-fragment budget.",
                "Messages",
                0,
            ),
        ),
        (
            "*9223372036854775808\n".to_owned(),
            located(int64_range, "Messages", 0),
        ),
        (
            "*2\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n".to_owned(),
            located(unknown, "Command", 3),
        ),
        (
            "*2\n*1\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n".to_owned(),
            located(unknown, "Command", 6),
        ),
        (
            "*2\n@NODE_PUB\n+node-1\n@SERVER_MSG\n:1\n$0\n\n".to_owned(),
            located(unknown, "Command", 3),
        ),
        ("+hello\n".to_owned(), located(unexpected, "Message", 0)),
        (":1\n".to_owned(), located(unexpected, "Message", 0)),
        ("$-1\n".to_owned(), located(unexpected, "Message", 0)),
        (
            "@SERVER_MSG\n:+1\n$0\n\n".to_owned(),
            located(digits, "Timestamp", 12),
        ),
        (
            "@SERVER_MSG\n: 1\n$0\n\n".to_owned(),
            located(digits, "Timestamp", 12),
        ),
        (
            "@SERVER_MSG\n:1.0\n$0\n\n".to_owned(),
            located(digits, "Timestamp", 12),
        ),
        (
            "@SERVER_MSG\n:9223372036854775808\n$0\n\n".to_owned(),
            located(int64_range, "Timestamp", 12),
        ),
        (
            "@SERVER_MSG\n:-9223372036854775809\n$0\n\n".to_owned(),
            located(int64_range, "Timestamp", 12),
        ),
        (
            "@SERVER_MSG\n:1\n$-2\n".to_owned(),
            located("Invalid bulk byte length.", "Payload", 15),
        ),
        (
            "@SERVER_MSG\n:1\n$-1\n".to_owned(),
            located("Payload cannot be null.", "Payload", 15),
        ),
        (
            "@SERVER_MSG\n:1\n$9999999999999999999\n".to_owned(),
            located(int64_range, "Payload", 15),
        ),
        (
            "@SERVER_MSG\n:1\n$2\nx\n".to_owned(),
            located(terminator, "Payload", 15),
        ),
        (
            "@SERVER_MSG\n:1\n$1\nx!".to_owned(),
            located(terminator, "Payload", 15),
        ),
        (
            "@SERVER_MSG\n:1\n$0\n\r!".to_owned(),
            located(terminator, "Payload", 15),
        ),
        (
            "@MSG\n+\n+s\n$-1\n:1\n$0\n\n".to_owned(),
            located(identifier, "TokenReference", 5),
        ),
        (
            "@MSG\n+u\n+s\n+\n:1\n$0\n\n".to_owned(),
            located(identifier, "MessageID", 11),
        ),
        (
            "@MSG\n+u\rX\n+s\n$-1\n:1\n$0\n\n".to_owned(),
            located(identifier, "TokenReference", 5),
        ),
        (
            "@MSG\n$3\nu\ns\n+s\n$-1\n:1\n$0\n\n".to_owned(),
            located(identifier, "TokenReference", 5),
        ),
        // A line-based layout without field markers.
        (
            "-Err\nParserError\nmessage".to_owned(),
            located(not_simple, "ErrorType", 5),
        ),
        (
            "-Other\n+ParserError\n$-1\n$1\nm\n$-1\n".to_owned(),
            located("Line exceeds its 3-byte limit.", "Error", 0),
        ),
        // Missing fields.
        (
            "-Err\n+ParserError\n$-1\n$1\nm\n".to_owned(),
            located(missing, "Resource", 27),
        ),
        (
            "-Err\n+ParserError\n$-1\n".to_owned(),
            located(missing, "ErrorMessage", 22),
        ),
        // The type must be a simple-string name.
        (
            "-Err\n$11\nParserError\n$-1\n$1\nm\n$-1\n".to_owned(),
            located(not_simple, "ErrorType", 5),
        ),
        (
            "-Err\n+Bad\rName\n$-1\n$1\nm\n$-1\n".to_owned(),
            located(invalid_name, "ErrorType", 5),
        ),
        (
            "-Err\n+Bad-Name\n$-1\n$1\nm\n$-1\n".to_owned(),
            located(invalid_name, "ErrorType", 5),
        ),
        (
            format!("-Err\n+{}\n$-1\n$1\nm\n$-1\n", "ParserError".repeat(6)),
            located("Line exceeds its 64-byte limit.", "ErrorType", 5),
        ),
        // The sub type must be a name or null.
        (
            "-Err\n+ParserError\n+PRES LIST\n$1\nm\n$-1\n".to_owned(),
            located(invalid_name, "ErrorSubType", 18),
        ),
        (
            "-Err\n+ParserError\n$3\nSUB\n$1\nm\n$-1\n".to_owned(),
            located(sub_type, "ErrorSubType", 18),
        ),
        (
            "-Err\n+ParserError\n:1\n$1\nm\n$-1\n".to_owned(),
            located(sub_type, "ErrorSubType", 18),
        ),
        // The message cannot be null.
        (
            "-Err\n+ParserError\n$-1\n$-1\n$-1\n".to_owned(),
            located("Payload cannot be null.", "ErrorMessage", 22),
        ),
        // The resource must be a known fragment, within the depth limit.
        (
            "-Err\n+ParserError\n$-1\n$1\nm\n@SERVER_MSG\n".to_owned(),
            located("Unexpected resource marker.", "Resource", 27),
        ),
        (
            format!("-Err\n+ParserError\n$-1\n$1\nm\n{}$-1\n", "*1\n".repeat(40)),
            located(deeper, "Resource", 120),
        ),
        (
            "@PRES_LIST_RESPONSE\n+s\n$1\n1\n;0\n;1\n;1\n;0\n;0\n*1\n*2\n+u\n+c\n".to_owned(),
            located(
                "Presence connection must contain three fields.",
                "Connection",
                46,
            ),
        ),
        (
            "@PRES_NOTIFY\n+s\n+u\n+c\n:1\n:123\n".to_owned(),
            located("Expected Integer32 marker.", "Event", 22),
        ),
    ];

    let mut vectors: Vec<(Vec<u8>, CelerisError)> = texts
        .into_iter()
        .map(|(wire, failure)| (wire.into_bytes(), failure))
        .collect();

    // Presence figures are Integer32: signed 32-bit, decimal digits only,
    // with the `;` marker.
    let totals = [
        (";2147483648\n", int32_range),
        (";-2147483649\n", int32_range),
        (";000000000001\n", "Line exceeds its 11-byte limit."),
        (";+1\n", digits),
        ("; 1\n", digits),
        (";1.0\n", digits),
        (":1\n", "Expected Integer32 marker."),
    ];

    for (total, message) in totals {
        vectors.push((
            format!("@PRES_LIST_RESPONSE\n+s\n$1\n1\n{total};1\n;1\n;0\n;0\n*0\n").into_bytes(),
            located(message, "Total", 28),
        ));
    }

    vectors.push((
        join(&[b"@MSG\n+", &[255], b"\n+s\n$-1\n:1\n$0\n\n"]),
        located("Invalid UTF-8 text.", "TokenReference", 5),
    ));

    for invalid in INVALID_UTF8 {
        vectors.push((
            join(&[
                format!("@MSG\n${}\n", invalid.len()).as_bytes(),
                invalid,
                b"\n+s\n$-1\n:1\n$0\n\n",
            ]),
            located("Invalid UTF-8 text.", "TokenReference", 5),
        ));
    }

    vectors
} // end fn malformed_vectors

/// Checks that a failure is a protocol error that the decoder can give, and
/// that its offset is inside the message.
fn assert_known_failure(result: Result<ServerMessage, CelerisError>, length: usize) {
    let Err(error) = result else {
        return;
    };

    let CelerisError::Protocol {
        message, offset, ..
    } = &error
    else {
        panic!("expected a protocol error, got {error:?}");
    };

    let normalized = message
        .split(|character: char| character.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("N");

    assert!(
        DECODER_REASONS.contains(&normalized.as_str()),
        "unexpected reason {message:?}"
    );
    assert!(*offset <= length, "offset {offset} past {length} bytes");
} // end fn assert_known_failure

#[test]
fn decoding_vectors_decode_exactly() {
    for (name, wire, expected) in decoding_vectors() {
        assert_eq!(decode_server_message(&wire), Ok(expected), "{name}");
    }
} // end fn decoding_vectors_decode_exactly

#[test]
fn malformed_vectors_fail_exactly() {
    for (wire, expected) in malformed_vectors() {
        assert_eq!(
            decode_server_message(&wire),
            Err(expected),
            "{:?}",
            String::from_utf8_lossy(&wire)
        );
    }
} // end fn malformed_vectors_fail_exactly

#[test]
fn each_server_error_type_keeps_every_field() {
    let types = [
        ("ParserError", ServerErrorType::ParserError),
        ("SendError", ServerErrorType::SendError),
        (
            "PermissionDeniedError",
            ServerErrorType::PermissionDeniedError,
        ),
        ("RateLimitError", ServerErrorType::RateLimitError),
        (
            "MessageSizeLimitError",
            ServerErrorType::MessageSizeLimitError,
        ),
        ("InternalError", ServerErrorType::InternalError),
        (
            "NewerError",
            ServerErrorType::Other("NewerError".to_owned()),
        ),
    ];

    for (name, error_type) in types {
        let wire = format!("-Err\n+{name}\n+PUB\n$7\nmessage\n$4\nroom\n");

        assert_eq!(
            decode_server_message(wire.as_bytes()),
            Ok(server_error(
                error_type,
                Some("PUB"),
                "message",
                text("room")
            )),
            "{name}"
        );
    }
} // end fn each_server_error_type_keeps_every_field

#[test]
fn a_malformed_error_message_is_still_delivered() {
    // ERR-01: the server's text is read without failing on bad bytes.
    let wire = join(&[b"-Err\n+ParserError\n$-1\n$3\nf", &[0xff, b'!'], b"\n$-1\n"]);

    assert_eq!(
        decode_server_message(&wire),
        Ok(server_error(
            ServerErrorType::ParserError,
            None,
            "f\u{FFFD}!",
            ServerErrorResource::Null
        ))
    );
} // end fn a_malformed_error_message_is_still_delivered

#[test]
fn each_missing_error_field_is_a_failure() {
    let cases: [(&str, CelerisError); 5] = [
        ("-Err", located("Unterminated line.", "Error", 0)),
        ("-Err\n", located("Missing field marker.", "ErrorType", 5)),
        (
            "-Err\n+ParserError\n",
            located("Missing field marker.", "ErrorSubType", 18),
        ),
        (
            "-Err\n+ParserError\n$-1\n",
            located("Missing field marker.", "ErrorMessage", 22),
        ),
        (
            "-Err\n+ParserError\n$-1\n$1\nm\n",
            located("Missing field marker.", "Resource", 27),
        ),
    ];

    for (wire, expected) in cases {
        assert_eq!(
            decode_server_message(wire.as_bytes()),
            Err(expected),
            "{wire:?}"
        );
    }
} // end fn each_missing_error_field_is_a_failure

#[test]
fn errors_decode_anywhere_in_a_batch() {
    let error = "-Err\n+RateLimitError\n$-1\n$4\nslow\n$-1\n";
    let wire = format!("*2\n*3\n@SERVER_MSG\n:1\n$1\na\n{error}@SERVER_MSG\n:2\n$1\nb\n{error}");
    let rate_limit = server_error(
        ServerErrorType::RateLimitError,
        None,
        "slow",
        ServerErrorResource::Null,
    );

    assert_eq!(
        decode_server_message(wire.as_bytes()),
        Ok(ServerMessage::Array(vec![
            ServerMessage::Array(vec![notice(1, b"a"), rate_limit.clone(), notice(2, b"b")]),
            rate_limit,
        ]))
    );
} // end fn errors_decode_anywhere_in_a_batch

#[test]
fn an_unknown_command_is_skipped_only_in_tail_position() {
    // The last element of every enclosing array is a tail.
    assert_eq!(
        decode_server_message(b"*1\n*2\n*0\n@FUTURE\n+a\n:1\n"),
        Ok(ServerMessage::Array(vec![ServerMessage::Array(vec![
            ServerMessage::Array(vec![]),
            ServerMessage::Ignored,
        ])]))
    );

    // The last element of an inner array that is not itself last is not.
    assert_eq!(
        decode_server_message(b"*2\n*1\n@FUTURE\n+a\n*0\n"),
        Err(located(
            "Unknown command inside array has ambiguous boundaries.",
            "Command",
            6
        ))
    );

    // Like the reference, a command name longer than the longest the server
    // sends is a failure, not a skip.
    assert_eq!(
        decode_server_message(b"@FUTURE_COMMAND_NAME\n+a\n"),
        Err(located("Line exceeds its 18-byte limit.", "Command", 0))
    );

    assert_eq!(
        decode_server_message(&[b'@', 0xff, b'\n']),
        Err(located("Invalid UTF-8 text.", "Command", 0))
    );
} // end fn an_unknown_command_is_skipped_only_in_tail_position

#[test]
fn failures_are_located_and_displayed() {
    let cases: Vec<(String, &str, &'static str, usize)> = vec![
        (
            "?".to_owned(),
            "Unexpected server message marker.",
            "Message",
            0,
        ),
        (
            "*0\n!".to_owned(),
            "Trailing data after server message.",
            "Message",
            3,
        ),
        (
            "*2\n@UNKNOWN\n@SERVER_MSG\n:1\n$0\n\n".to_owned(),
            "Unknown command inside array has ambiguous boundaries.",
            "Command",
            3,
        ),
        (
            "@SERVER_MSG\n:abc\n".to_owned(),
            "Expected decimal digits.",
            "Timestamp",
            12,
        ),
        (
            "@SERVER_MSG\n:000000000000000000000\n".to_owned(),
            "Line exceeds its 20-byte limit.",
            "Timestamp",
            12,
        ),
        (
            "@SERVER_MSG\n:1".to_owned(),
            "Unterminated line.",
            "Timestamp",
            12,
        ),
        (
            "@SERVER_MSG\n+1\n".to_owned(),
            "Expected Integer64 marker.",
            "Timestamp",
            12,
        ),
        (
            "@SERVER_MSG\n:1\n$9\nx\n".to_owned(),
            "Bulk payload exceeds remaining message bytes.",
            "Payload",
            15,
        ),
        (
            "@SERVER_MSG\n:1\n*0\n".to_owned(),
            "Expected simple or bulk byte marker.",
            "Payload",
            15,
        ),
        (
            "@MSG\n+u\n+\n".to_owned(),
            "Identifier must be nonempty and CR/LF-free.",
            "SegmentID",
            8,
        ),
        (
            "@MSG\n$-1\n".to_owned(),
            "Identifier cannot be null.",
            "TokenReference",
            5,
        ),
        ("-Bad\n".to_owned(), "Invalid error header.", "Error", 0),
        (
            "-Err\n+Bad Name\n$-1\n$6\nsecret\n$-1\n".to_owned(),
            "Invalid error name.",
            "ErrorType",
            5,
        ),
        (
            "-Err\n+ParserError\n$-1\n$6\nsecret\n@X\n".to_owned(),
            "Unexpected resource marker.",
            "Resource",
            32,
        ),
        (
            format!("{}*0\n", "*1\n".repeat(32)),
            "Arrays are nested deeper than 32 levels.",
            "Messages",
            96,
        ),
        (
            "@PRES_NOTIFY\n+s\n+u\n+c\n;2\n:1\n".to_owned(),
            "Presence event must be 0 or 1.",
            "Event",
            22,
        ),
    ];

    for (wire, message, field, offset) in cases {
        let error = decode_server_message(wire.as_bytes()).unwrap_err();
        assert_eq!(error, located(message, field, offset), "{wire:?}");

        assert_eq!(
            error.to_string(),
            format!("{message} Field: {field}, byte offset {offset}.")
        );
    }
} // end fn failures_are_located_and_displayed

#[test]
fn a_failure_never_repeats_received_bytes() {
    let wire = join(&[b"@MSG\n+", &[255], b"synthetic-secret\n"]);
    let error = decode_server_message(&wire).unwrap_err();
    assert_eq!(error, located("Invalid UTF-8 text.", "TokenReference", 5));
    assert!(!error.to_string().contains("synthetic-secret"));
    assert!(!format!("{error:?}").contains("synthetic-secret"));

    let error = decode_server_message(b"synthetic-secret").unwrap_err();
    assert_eq!(
        error,
        located("Unexpected server message marker.", "Message", 0)
    );
    assert!(!error.to_string().contains("synthetic-secret"));
} // end fn a_failure_never_repeats_received_bytes

/// The exact failure for each prefix of `@MSG\n+user\n+chat\n$-1\n:0\n$0\n\n`.
fn truncation_failure(length: usize) -> CelerisError {
    let (field, start) = match length {
        0 => ("Message", 0),
        1..=4 => ("Command", 0),
        5..=10 => ("TokenReference", 5),
        11..=16 => ("SegmentID", 11),
        17..=20 => ("MessageID", 17),
        21..=23 => ("Timestamp", 21),
        _ => ("Payload", 24),
    };

    let message = match length {
        0 | 5 | 11 | 17 | 21 | 24 => "Missing field marker.",
        27 => "Missing bulk byte terminator.",
        _ => "Unterminated line.",
    };

    located(message, field, start)
} // end fn truncation_failure

#[test]
fn every_truncation_of_a_delivery_fails_exactly() {
    let wire = b"@MSG\n+user\n+chat\n$-1\n:0\n$0\n\n";
    assert_eq!(wire.len(), 28);

    for length in 0..wire.len() {
        assert_eq!(
            decode_server_message(&wire[..length]),
            Err(truncation_failure(length)),
            "length {length}"
        );
    }
} // end fn every_truncation_of_a_delivery_fails_exactly

#[test]
fn every_truncation_of_every_vector_fails() {
    let mut checked = 0;

    for (name, wire, expected) in decoding_vectors() {
        // An unknown command runs to the end of the message, so a shorter
        // message still skips it.
        if format!("{expected:?}").contains("Ignored") {
            continue;
        }

        for length in 0..wire.len() {
            let result = decode_server_message(&wire[..length]);
            assert!(
                matches!(result, Err(CelerisError::Protocol { .. })),
                "{name} at {length}: {result:?}"
            );
            assert_known_failure(result, length);
            checked += 1;
        }
    }

    assert!(checked > 600, "only {checked} truncations checked");
} // end fn every_truncation_of_every_vector_fails

#[test]
fn trailing_bytes_after_any_vector_fail() {
    for (name, wire, expected) in decoding_vectors() {
        if format!("{expected:?}").contains("Ignored") {
            continue;
        }

        let mut extended = wire.clone();
        extended.push(b'\n');

        assert_eq!(
            decode_server_message(&extended),
            Err(located(
                "Trailing data after server message.",
                "Message",
                wire.len()
            )),
            "{name}"
        );
    }
} // end fn trailing_bytes_after_any_vector_fail

#[test]
fn mutated_inputs_decode_or_fail_cleanly() {
    let vectors = decoding_vectors();
    let mut seed: u32 = 0xce1e;
    let mut failures = 0;

    for attempt in 0..512 {
        let mut wire = vectors[attempt % vectors.len()].1.clone();
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        let index = seed as usize % wire.len();
        wire[index] = (seed & 255) as u8;

        let result = decode_server_message(&wire);
        failures += usize::from(result.is_err());
        assert_known_failure(result, wire.len());
    }

    assert!(failures > 0, "no mutation failed");
} // end fn mutated_inputs_decode_or_fail_cleanly

#[test]
fn every_single_byte_mutation_decodes_or_fails_cleanly() {
    let replacements = [
        0x00, b'\n', b'\r', b'*', b'$', b'-', b'+', b':', b';', b'9', 0xff,
    ];

    for (_, wire, _) in decoding_vectors() {
        for index in 0..wire.len() {
            for replacement in replacements {
                let mut mutated = wire.clone();
                mutated[index] = replacement;

                assert_known_failure(decode_server_message(&mutated), mutated.len());
            }
        }
    }
} // end fn every_single_byte_mutation_decodes_or_fails_cleanly

#[test]
fn nesting_is_bounded_at_32_levels() {
    let mut expected = ServerMessage::Array(vec![]);

    for _ in 0..31 {
        expected = ServerMessage::Array(vec![expected]);
    }

    assert_eq!(
        decode_server_message(format!("{}*0\n", "*1\n".repeat(31)).as_bytes()),
        Ok(expected)
    );

    assert_eq!(
        decode_server_message(format!("{}*0\n", "*1\n".repeat(32)).as_bytes()),
        Err(located(
            "Arrays are nested deeper than 32 levels.",
            "Messages",
            96
        ))
    );
} // end fn nesting_is_bounded_at_32_levels

#[test]
fn fragments_are_bounded_at_4096() {
    // The outer array and 4095 empty arrays: 4096 fragments.
    let wire = format!("*4095\n{}", "*0\n".repeat(4095));
    let Ok(ServerMessage::Array(messages)) = decode_server_message(wire.as_bytes()) else {
        panic!("4096 fragments must decode");
    };

    assert_eq!(messages.len(), 4095);

    // Each notice is three fragments: 1 + 3 × 1365 = 4096.
    let notice_wire = "@SERVER_MSG\n:1\n$0\n\n";
    let wire = format!("*1365\n{}", notice_wire.repeat(1365));
    let Ok(ServerMessage::Array(messages)) = decode_server_message(wire.as_bytes()) else {
        panic!("4096 fragments must decode");
    };

    assert_eq!(messages.len(), 1365);

    // One notice more: the 4097th fragment is the marker of notice 1366.
    let wire = format!("*1366\n{}", notice_wire.repeat(1366));

    assert_eq!(
        decode_server_message(wire.as_bytes()),
        Err(located(
            "Server message has more than 4096 fragments.",
            "Message",
            6 + 1365 * 19
        ))
    );
} // end fn fragments_are_bounded_at_4096

#[test]
fn messages_larger_than_one_mebibyte_decode() {
    // LIMIT-01: a full prime-plan delivery, and one larger than the 2 MiB
    // outbound limit, are never size-checked on receipt.
    for length in [1024 * 1024, 3 * 1024 * 1024] {
        let payload: Vec<u8> = (0..length).map(|index| (index % 251) as u8).collect();
        let wire = join(&[
            format!("@MSG\n+user\n+chat\n+msg_1\n:1\n${length}\n").as_bytes(),
            &payload,
            b"\n",
        ]);

        assert!(wire.len() > 1024 * 1024);

        assert_eq!(
            decode_server_message(&wire),
            Ok(delivery("user", "chat", Some("msg_1"), 1, &payload))
        );
    }
} // end fn messages_larger_than_one_mebibyte_decode

#[test]
fn the_shared_test_frames_decode_to_their_values() {
    // WIRE-05: the frames that the channel tests send are built by hand in
    // the support module, apart from the decoder.
    assert_eq!(
        decode_server_message(&support::delivery_frame("alice", "chat", "m1", 7, b"\0\n")),
        Ok(delivery("alice", "chat", Some("m1"), 7, b"\0\n"))
    );

    assert_eq!(
        decode_server_message(&support::notice_frame(-3, "hello")),
        Ok(notice(-3, b"hello"))
    );

    assert_eq!(
        decode_server_message(&support::presence_frame(
            "chat",
            "user",
            "connection",
            false,
            124
        )),
        Ok(presence_notify(false, 124))
    );

    assert_eq!(
        decode_server_message(&support::presence_list_frame(
            "chat",
            "7",
            [1, 25, 1, 1, 1],
            &[("user", "connection", 123)]
        )),
        Ok(presence_list(
            "7",
            [1, 25, 1, 1, 1],
            vec![PresenceConnection {
                token_reference: "user".to_owned(),
                connection_id: "connection".to_owned(),
                timestamp: 123,
            }]
        ))
    );

    assert_eq!(
        decode_server_message(&support::error_frame(
            "PermissionDeniedError",
            Some("SUB"),
            "denied",
            Some("chat")
        )),
        Ok(server_error(
            ServerErrorType::PermissionDeniedError,
            Some("SUB"),
            "denied",
            text("chat")
        ))
    );

    assert_eq!(
        decode_server_message(&support::error_frame("RateLimitError", None, "slow", None)),
        Ok(server_error(
            ServerErrorType::RateLimitError,
            None,
            "slow",
            ServerErrorResource::Null
        ))
    );
} // end fn the_shared_test_frames_decode_to_their_values
