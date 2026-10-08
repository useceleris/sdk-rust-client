//! Decoder boundaries (WIRE-03, WIRE-04, INT-01): each limit is checked at its
//! value and one step past it, with the exact failure.

use crate::decode::decode_server_message;
use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::messages::{Delivery, PresenceListResponse, ServerMessage};
use crate::values::{PresenceConnection, PresenceEvent, PresencePage, ServerNotice};

/// An error frame up to its resource: 27 bytes, so the resource starts at
/// offset 27.
const ERROR_HEAD: &str = "-Err\n+ParserError\n$-1\n$1\nm\n";

/// A presence response up to its connections: 43 bytes.
const PRESENCE_HEAD: &str = "@PRES_LIST_RESPONSE\n+s\n$1\n1\n;1\n;1\n;1\n;1\n;1\n";

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

fn decode(wire: &str) -> Result<ServerMessage, CelerisError> {
    decode_server_message(wire.as_bytes())
} // end fn decode

fn notice(timestamp: i64, payload: &[u8]) -> ServerMessage {
    ServerMessage::Notice(ServerNotice {
        timestamp,
        payload: payload.to_vec(),
    })
} // end fn notice

fn parser_error(resource: ServerErrorResource) -> ServerMessage {
    ServerMessage::Error(ServerError {
        error_type: ServerErrorType::ParserError,
        sub_type: None,
        message: "m".to_owned(),
        resource,
    })
} // end fn parser_error

fn named_error(error_type: &str, sub_type: Option<&str>) -> ServerMessage {
    ServerMessage::Error(ServerError {
        error_type: ServerErrorType::Other(error_type.to_owned()),
        sub_type: sub_type.map(str::to_owned),
        message: "m".to_owned(),
        resource: ServerErrorResource::Null,
    })
} // end fn named_error

fn page(figures: [i32; 5], connections: Vec<PresenceConnection>) -> ServerMessage {
    let [total, per_page, current_page, from, to] = figures;

    ServerMessage::PresenceList(PresenceListResponse {
        request_id: "1".to_owned(),
        page: PresencePage {
            segment_id: "s".to_owned(),
            total,
            per_page,
            current_page,
            from,
            to,
            connections,
        },
    })
} // end fn page

fn connection(timestamp: i64) -> PresenceConnection {
    PresenceConnection {
        token_reference: "u".to_owned(),
        connection_id: "c".to_owned(),
        timestamp,
    }
} // end fn connection

fn nested(levels: usize, message: ServerMessage) -> ServerMessage {
    let mut nested = message;

    for _ in 0..levels {
        nested = ServerMessage::Array(vec![nested]);
    }

    nested
} // end fn nested

#[test]
fn maximum_length_headers_are_accepted_with_lf_and_crlf() {
    for newline in ["\n", "\r\n"] {
        assert_eq!(
            decode(&format!(
                "@SERVER_MSG{newline}:00000000000000000001{newline}$0{newline}{newline}"
            )),
            Ok(notice(1, b"")),
            "{newline:?}"
        );

        let error_type = "A".repeat(64);
        let sub_type = "B".repeat(64);

        assert_eq!(
            decode(&format!(
                "-Err{newline}+{error_type}{newline}+{sub_type}{newline}$1{newline}m{newline}$-1{newline}"
            )),
            Ok(named_error(&error_type, Some(&sub_type))),
            "{newline:?}"
        );
    }
} // end fn maximum_length_headers_are_accepted_with_lf_and_crlf

#[test]
fn bounded_headers_fail_exactly() {
    let unterminated = "Unterminated line.";
    let too_long = "Line exceeds its 20-byte limit.";

    let cases = [
        ("0".repeat(20), unterminated),
        ("0".repeat(21), unterminated),
        ("0".repeat(22), too_long),
        (format!("{}\n", "0".repeat(21)), too_long),
        (format!("{}\r\n", "0".repeat(21)), too_long),
        // A long header fails without a search of the rest of the message.
        (format!("{}\n", "0".repeat(65536)), too_long),
    ];

    for (header, message) in cases {
        assert_eq!(
            decode(&format!("@SERVER_MSG\n:{header}")),
            Err(located(message, "Timestamp", 12)),
            "{} bytes",
            header.len()
        );
    }

    // An error name is bounded at 64 bytes, and a sub type too.
    assert_eq!(
        decode(&format!("-Err\n+{}\n$-1\n$1\nm\n$-1\n", "A".repeat(65))),
        Err(located("Line exceeds its 64-byte limit.", "ErrorType", 5))
    );

    assert_eq!(
        decode(&format!(
            "-Err\n+ParserError\n+{}\n$1\nm\n$-1\n",
            "B".repeat(65)
        )),
        Err(located(
            "Line exceeds its 64-byte limit.",
            "ErrorSubType",
            18
        ))
    );
} // end fn bounded_headers_fail_exactly

#[test]
fn integer64_limits() {
    let range = "Integer is outside -9223372036854775808 to 9223372036854775807.";

    assert_eq!(
        decode(&format!("{ERROR_HEAD}:9223372036854775807\n")),
        Ok(parser_error(ServerErrorResource::Integer64(i64::MAX)))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD}:-9223372036854775808\n")),
        Ok(parser_error(ServerErrorResource::Integer64(i64::MIN)))
    );

    for value in ["9223372036854775808", "-9223372036854775809"] {
        assert_eq!(
            decode(&format!("{ERROR_HEAD}:{value}\n")),
            Err(located(range, "Resource", 27)),
            "{value}"
        );
    }

    // A presence connection's timestamp is an Integer64 too.
    assert_eq!(
        decode(&format!(
            "{PRESENCE_HEAD}*1\n*3\n+u\n+c\n:9223372036854775807\n"
        )),
        Ok(page([1; 5], vec![connection(i64::MAX)]))
    );

    assert_eq!(
        decode(&format!(
            "{PRESENCE_HEAD}*1\n*3\n+u\n+c\n:-9223372036854775809\n"
        )),
        Err(located(range, "Timestamp", 55))
    );

    // Leading zeros fit while the line stays within 20 bytes.
    assert_eq!(
        decode("@SERVER_MSG\n:-0000000000000000001\n$0\n\n"),
        Ok(notice(-1, b""))
    );

    assert_eq!(
        decode("@SERVER_MSG\n:-00000000000000000001\n$0\n\n"),
        Err(located("Line exceeds its 20-byte limit.", "Timestamp", 12))
    );
} // end fn integer64_limits

#[test]
fn integer32_limits() {
    let range = "Integer is outside -2147483648 to 2147483647.";

    assert_eq!(
        decode(
            "@PRES_LIST_RESPONSE\n+s\n$1\n1\n;2147483647\n;-2147483648\n;0\n;2147483647\n;-1\n*0\n"
        ),
        Ok(page([i32::MAX, i32::MIN, 0, i32::MAX, -1], vec![]))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD};2147483647\n")),
        Ok(parser_error(ServerErrorResource::Integer32(i32::MAX)))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD};-2147483648\n")),
        Ok(parser_error(ServerErrorResource::Integer32(i32::MIN)))
    );

    for value in ["2147483648", "-2147483649"] {
        assert_eq!(
            decode(&format!("{ERROR_HEAD};{value}\n")),
            Err(located(range, "Resource", 27)),
            "{value}"
        );

        // The last figure, To, starts at offset 40.
        assert_eq!(
            decode(&format!(
                "@PRES_LIST_RESPONSE\n+s\n$1\n1\n;1\n;1\n;1\n;1\n;{value}\n*0\n"
            )),
            Err(located(range, "To", 40)),
            "{value}"
        );
    }

    assert_eq!(
        decode(&format!("{ERROR_HEAD};-02147483648\n")),
        Err(located("Line exceeds its 11-byte limit.", "Resource", 27))
    );
} // end fn integer32_limits

#[test]
fn the_presence_flag_is_zero_or_one() {
    let notify = |event: &str| decode(&format!("@PRES_NOTIFY\n+s\n+u\n+c\n{event}\n:5\n"));

    for (event, joined) in [(";1", true), (";0", false)] {
        assert_eq!(
            notify(event),
            Ok(ServerMessage::PresenceNotify(PresenceEvent {
                segment_id: "s".to_owned(),
                token_reference: "u".to_owned(),
                connection_id: "c".to_owned(),
                joined,
                timestamp: 5,
            }))
        );
    }

    for event in [";2", ";-1", ";2147483647"] {
        assert_eq!(
            notify(event),
            Err(located("Presence event must be 0 or 1.", "Event", 22)),
            "{event}"
        );
    }

    assert_eq!(
        notify(";2147483648"),
        Err(located(
            "Integer is outside -2147483648 to 2147483647.",
            "Event",
            22
        ))
    );
} // end fn the_presence_flag_is_zero_or_one

#[test]
fn the_wrong_integer_marker_fails() {
    assert_eq!(
        decode("@SERVER_MSG\n;1\n$0\n\n"),
        Err(located("Expected Integer64 marker.", "Timestamp", 12))
    );

    assert_eq!(
        decode(&format!("{PRESENCE_HEAD}*1\n*3\n+u\n+c\n;1\n")),
        Err(located("Expected Integer64 marker.", "Timestamp", 55))
    );

    assert_eq!(
        decode("@PRES_LIST_RESPONSE\n+s\n$1\n1\n;1\n:1\n;1\n;1\n;1\n*0\n"),
        Err(located("Expected Integer32 marker.", "PerPage", 31))
    );

    assert_eq!(
        decode("@PRES_NOTIFY\n+s\n+u\n+c\n:1\n:5\n"),
        Err(located("Expected Integer32 marker.", "Event", 22))
    );
} // end fn the_wrong_integer_marker_fails

#[test]
fn the_accepted_decimal_spellings() {
    for (text, value) in [("0001", 1), ("-0", 0), ("-0001", -1)] {
        assert_eq!(
            decode(&format!("@SERVER_MSG\n:{text}\n$0\n\n")),
            Ok(notice(value, b"")),
            "{text}"
        );
    }

    for text in [
        "", " ", "-", "0x10", "0o10", "0b10", "+1", "1 ", "\t1", "1\r\r", "--1", "1.5", "1e2", "١",
    ] {
        assert_eq!(
            decode(&format!("@SERVER_MSG\n:{text}\n$0\n\n")),
            Err(located("Expected decimal digits.", "Timestamp", 12)),
            "{text:?}"
        );
    }
} // end fn the_accepted_decimal_spellings

#[test]
fn malformed_counts_fail() {
    let digits = "Expected decimal digits.";

    assert_eq!(decode("*\n"), Err(located(digits, "Messages", 0)));
    assert_eq!(decode("*+1\n*0\n"), Err(located(digits, "Messages", 0)));

    assert_eq!(
        decode("*01\n*0\n"),
        Ok(ServerMessage::Array(vec![ServerMessage::Array(vec![])]))
    );

    assert_eq!(
        decode(&format!("{PRESENCE_HEAD}*-1\n")),
        Err(located(
            "Array length cannot be negative.",
            "Connections",
            43
        ))
    );

    assert_eq!(
        decode(&format!("{PRESENCE_HEAD}+x\n")),
        Err(located("Expected array marker.", "Connections", 43))
    );

    assert_eq!(
        decode(&format!("{PRESENCE_HEAD}*1\n+x\n")),
        Err(located("Expected array marker.", "Connection", 46))
    );

    assert_eq!(
        decode(&format!("{PRESENCE_HEAD}*1\n*4\n+u\n+c\n:1\n+x\n")),
        Err(located(
            "Presence connection must contain three fields.",
            "Connection",
            46
        ))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD}*-1\n")),
        Err(located("Array length cannot be negative.", "Resource", 27))
    );
} // end fn malformed_counts_fail

#[test]
fn an_error_resource_counts_toward_the_fragment_budget() {
    // Five fragments come before the resource's items: the message, type,
    // sub type, message text and resource markers.
    let wire = format!("{ERROR_HEAD}*4091\n{}", "$-1\n".repeat(4091));

    assert_eq!(
        decode(&wire),
        Ok(parser_error(ServerErrorResource::Array(vec![
            ServerErrorResource::Null;
            4091
        ])))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD}*4092\n")),
        Err(located(
            "Array length exceeds the 4096-fragment budget.",
            "Resource",
            27
        ))
    );
} // end fn an_error_resource_counts_toward_the_fragment_budget

#[test]
fn hostile_lengths_fail() {
    let exceeds = "Bulk payload exceeds remaining message bytes.";

    assert_eq!(
        decode("@SERVER_MSG\n:1\n$9223372036854775807\nx\n"),
        Err(located(exceeds, "Payload", 15))
    );

    // One byte more than remains.
    assert_eq!(
        decode("@SERVER_MSG\n:1\n$2\nx"),
        Err(located(exceeds, "Payload", 15))
    );

    // Exactly what remains, so no terminator is left.
    assert_eq!(
        decode("@SERVER_MSG\n:1\n$1\nx"),
        Err(located("Missing bulk byte terminator.", "Payload", 15))
    );

    assert_eq!(
        decode("@MSG\n$9223372036854775807\nu\n"),
        Err(located(exceeds, "TokenReference", 5))
    );

    assert_eq!(
        decode("*9223372036854775807\n"),
        Err(located(
            "Array length exceeds the 4096-fragment budget.",
            "Messages",
            0
        ))
    );

    assert_eq!(
        decode("@SERVER_MSG\n:1\n$00000000000000000001\nx\n"),
        Ok(notice(1, b"x"))
    );

    assert_eq!(
        decode("@SERVER_MSG\n:1\n$000000000000000000001\nx\n"),
        Err(located("Line exceeds its 20-byte limit.", "Payload", 15))
    );
} // end fn hostile_lengths_fail

#[test]
fn cr_is_allowed_only_before_lf() {
    assert_eq!(
        decode("@MSG\r\n+u\r\n$1\r\ns\r\n+m\r\n:1\r\n$1\r\nx\r\n"),
        Ok(ServerMessage::Delivery(Delivery {
            token_reference: "u".to_owned(),
            segment_id: "s".to_owned(),
            message_id: Some("m".to_owned()),
            timestamp: 1,
            payload: b"x".to_vec(),
        }))
    );

    // A bare CR does not end a line.
    assert_eq!(
        decode("@SERVER_MSG\r:1\r$0\r\r"),
        Err(located("Unterminated line.", "Command", 0))
    );

    // Only one CR before the LF is removed.
    assert_eq!(
        decode("@SERVER_MSG\n:1\r\r\n$0\n\n"),
        Err(located("Expected decimal digits.", "Timestamp", 12))
    );

    // A payload keeps its own CR and LF bytes.
    assert_eq!(
        decode("@SERVER_MSG\n:1\n+da\rta\r\n"),
        Ok(notice(1, b"da\rta"))
    );

    assert_eq!(
        decode("@SERVER_MSG\n:1\n$2\n\r\n\r\n"),
        Ok(notice(1, b"\r\n"))
    );

    assert_eq!(
        decode("@SERVER_MSG\n:1\n$1\nx\r!"),
        Err(located("Missing bulk byte terminator.", "Payload", 15))
    );
} // end fn cr_is_allowed_only_before_lf

#[test]
fn identifiers_refuse_cr_and_lf() {
    let rule = "Identifier must be nonempty and CR/LF-free.";

    assert_eq!(
        decode("@MSG\n$2\nu\r\n+s\n$-1\n:1\n$0\n\n"),
        Err(located(rule, "TokenReference", 5))
    );

    assert_eq!(
        decode("@MSG\n+u\n$3\ns\ns\n$-1\n:1\n$0\n\n"),
        Err(located(rule, "SegmentID", 8))
    );

    assert_eq!(
        decode("@PRES_NOTIFY\n+s\n+u\n$2\n\rc\n;1\n:1\n"),
        Err(located(rule, "ConnectionID", 19))
    );

    assert_eq!(
        decode("@PRES_LIST_RESPONSE\n+s\n$0\n\n"),
        Err(located(rule, "RequestID", 23))
    );
} // end fn identifiers_refuse_cr_and_lf

#[test]
fn null_identifiers_and_payloads_fail() {
    let null = "Identifier cannot be null.";

    assert_eq!(
        decode("@MSG\n+u\n$-1\n"),
        Err(located(null, "SegmentID", 8))
    );

    assert_eq!(
        decode("@PRES_NOTIFY\n+s\n+u\n$-1\n"),
        Err(located(null, "ConnectionID", 19))
    );

    assert_eq!(
        decode("@PRES_LIST_RESPONSE\n+s\n$-1\n"),
        Err(located(null, "RequestID", 23))
    );

    assert_eq!(
        decode("@MSG\n+u\n+s\n+m\n:1\n$-1\n"),
        Err(located("Payload cannot be null.", "Payload", 17))
    );
} // end fn null_identifiers_and_payloads_fail

#[test]
fn invalid_utf8_in_text_fields_fails() {
    let invalid = "Invalid UTF-8 text.";
    let bad: &[u8] = &[0xc0, 0xaf];

    let cases: Vec<(Vec<u8>, &'static str, usize)> = vec![
        (
            join(&[b"@MSG\n+u\n+", bad, b"\n+m\n:1\n$0\n\n"]),
            "SegmentID",
            8,
        ),
        (
            join(&[b"@MSG\n+u\n+s\n$2\n", bad, b"\n:1\n$0\n\n"]),
            "MessageID",
            11,
        ),
        (
            join(&[b"@PRES_NOTIFY\n+s\n+u\n+", bad, b"\n;1\n:1\n"]),
            "ConnectionID",
            19,
        ),
        (
            join(&[b"@PRES_LIST_RESPONSE\n+s\n$2\n", bad, b"\n"]),
            "RequestID",
            23,
        ),
        (
            join(&[PRESENCE_HEAD.as_bytes(), b"*1\n*3\n+", bad, b"\n+c\n:1\n"]),
            "TokenReference",
            49,
        ),
        (
            join(&[ERROR_HEAD.as_bytes(), b"+", bad, b"\n"]),
            "Resource",
            27,
        ),
        (
            join(&[ERROR_HEAD.as_bytes(), b"$2\n", bad, b"\n"]),
            "Resource",
            27,
        ),
    ];

    for (wire, field, offset) in cases {
        assert_eq!(
            decode_server_message(&wire),
            Err(located(invalid, field, offset)),
            "{field}"
        );
    }
} // end fn invalid_utf8_in_text_fields_fails

#[test]
fn error_name_character_class_edges() {
    for name in ["A", "Z", "a", "z", "Az09_"] {
        assert_eq!(
            decode(&format!("-Err\n+{name}\n+{name}\n$1\nm\n$-1\n")),
            Ok(named_error(name, Some(name))),
            "{name}"
        );
    }

    for name in [
        "", "@", "[", "`", "{", "0A", "_A", "A/", "A:", "A-", "A B", "é",
    ] {
        assert_eq!(
            decode(&format!("-Err\n+{name}\n$-1\n$1\nm\n$-1\n")),
            Err(located("Invalid error name.", "ErrorType", 5)),
            "{name:?}"
        );

        assert_eq!(
            decode(&format!("-Err\n+ParserError\n+{name}\n$1\nm\n$-1\n")),
            Err(located("Invalid error name.", "ErrorSubType", 18)),
            "{name:?}"
        );
    }
} // end fn error_name_character_class_edges

#[test]
fn presence_connections_count_toward_the_depth_limit() {
    let response = format!("{PRESENCE_HEAD}*1\n*3\n+u\n+c\n:1\n");

    // The connection is an array at depth 31.
    assert_eq!(
        decode(&format!("{}{response}", "*1\n".repeat(30))),
        Ok(nested(30, page([1; 5], vec![connection(1)])))
    );

    // At depth 32 it is refused; it starts at 93 + 46.
    assert_eq!(
        decode(&format!("{}{response}", "*1\n".repeat(31))),
        Err(located(
            "Arrays are nested deeper than 32 levels.",
            "Connection",
            139
        ))
    );
} // end fn presence_connections_count_toward_the_depth_limit

#[test]
fn error_resources_count_toward_the_depth_limit() {
    let mut resource = ServerErrorResource::Null;

    for _ in 0..31 {
        resource = ServerErrorResource::Array(vec![resource]);
    }

    assert_eq!(
        decode(&format!("{ERROR_HEAD}{}$-1\n", "*1\n".repeat(31))),
        Ok(parser_error(resource))
    );

    assert_eq!(
        decode(&format!("{ERROR_HEAD}{}$-1\n", "*1\n".repeat(32))),
        Err(located(
            "Arrays are nested deeper than 32 levels.",
            "Resource",
            120
        ))
    );

    // An error inside arrays counts their depth too.
    let inside = format!("{}{ERROR_HEAD}", "*1\n".repeat(31));

    assert_eq!(
        decode(&format!("{inside}$-1\n")),
        Ok(nested(31, parser_error(ServerErrorResource::Null)))
    );

    assert_eq!(
        decode(&format!("{inside}*0\n")),
        Err(located(
            "Arrays are nested deeper than 32 levels.",
            "Resource",
            120
        ))
    );
} // end fn error_resources_count_toward_the_depth_limit

#[test]
fn payloads_with_marker_bytes_decode_unchanged() {
    let pattern = b"\n\r@$*:+-";
    let payload: Vec<u8> = (0..65536).map(|index| pattern[index % 8]).collect();
    let wire = join(&[b"@SERVER_MSG\n:1\n$65536\n", &payload, b"\n"]);

    assert_eq!(decode_server_message(&wire), Ok(notice(1, &payload)));
} // end fn payloads_with_marker_bytes_decode_unchanged
