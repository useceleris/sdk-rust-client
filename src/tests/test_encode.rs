//! Encoder golden vectors and input rules (WIRE-01, WIRE-02, WIRE-05, PUB-03,
//! PUB-04, LIMIT-01). Every expected byte string is written by hand, from the
//! reference's codec vectors. A `NODE_*` command cannot be sent, because
//! `SegmentCommand` has no variant for one, so no test can build it.

use tokio_tungstenite::tungstenite::Bytes;

use crate::encode::{SegmentCommand, encode_presence_list, encode_publish, encode_segment_command};
use crate::errors::CelerisError;
use crate::identifiers::identifier_rule;
use crate::tests::support::segment_command;

const LIMIT: usize = 2 * 1024 * 1024;

const TOO_LARGE: &str = "Encoded command exceeds 2 MiB. That is the most the server accepts on any plan; send a smaller payload.";

fn bytes(result: Result<Bytes, CelerisError>) -> Vec<u8> {
    result.expect("the command is valid").to_vec()
} // end fn bytes

fn join(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
} // end fn join

fn configuration(message: &str) -> Result<Bytes, CelerisError> {
    Err(CelerisError::Configuration(message.to_owned()))
} // end fn configuration

#[test]
fn publish_vectors() {
    assert_eq!(
        bytes(encode_publish("default", None, b"hello")),
        b"@PUB\n$7\ndefault\n$-1\n$5\nhello\n"
    );

    assert_eq!(
        bytes(encode_publish("c:é", Some("識"), &[0, 255, 13, 10, 64])),
        join(&[
            "@PUB\n$4\nc:é\n$3\n識\n$5\n".as_bytes(),
            &[0, 255, 13, 10, 64, 10]
        ])
    );

    assert_eq!(
        bytes(encode_publish("a", None, b"")),
        b"@PUB\n$1\na\n$-1\n$0\n\n"
    );
} // end fn publish_vectors

#[test]
fn segment_command_vectors() {
    let vectors = [
        (SegmentCommand::Subscribe, "@SUB\n$4\nchat\n"),
        (SegmentCommand::Unsubscribe, "@UNSUB\n$4\nchat\n"),
        (SegmentCommand::PresenceSubscribe, "@PRES_SUB\n$4\nchat\n"),
        (
            SegmentCommand::PresenceUnsubscribe,
            "@PRES_UNSUB\n$4\nchat\n",
        ),
    ];

    for (command, expected) in vectors {
        assert_eq!(
            bytes(encode_segment_command(command, "chat")),
            expected.as_bytes(),
            "{command:?}"
        );

        // The channel tests expect the frame that the support module builds.
        assert_eq!(
            segment_command(command.name(), "chat").as_bytes(),
            expected.as_bytes()
        );
    }
} // end fn segment_command_vectors

#[test]
fn presence_list_vectors() {
    assert_eq!(
        bytes(encode_presence_list("chat", 1, 1, "1")),
        b"@PRES_LIST\n$4\nchat\n;1\n;1\n$1\n1\n"
    );

    assert_eq!(
        bytes(encode_presence_list("chat", 2147483647, 100, "識-9")),
        "@PRES_LIST\n$4\nchat\n;2147483647\n;100\n$5\n識-9\n".as_bytes()
    );
} // end fn presence_list_vectors

#[test]
fn unicode_identifiers_are_sent_unchanged() {
    let vectors = [
        ("😀", 4),
        ("\u{10000}", 4),
        ("\u{10FFFF}", 4),
        ("\u{FFFD}", 3),
        ("e\u{301}", 3),
        ("é", 2),
        ("a\0b", 3),
    ];

    for (identifier, length) in vectors {
        assert_eq!(
            bytes(encode_segment_command(
                SegmentCommand::Subscribe,
                identifier
            )),
            format!("@SUB\n${length}\n{identifier}\n").into_bytes(),
            "{identifier:?}"
        );
    }
} // end fn unicode_identifiers_are_sent_unchanged

#[test]
fn every_byte_value_is_sent_unchanged() {
    let payload: Vec<u8> = (0..=255).collect();

    assert_eq!(
        bytes(encode_publish("s", Some("m"), &payload)),
        join(&[b"@PUB\n$1\ns\n$1\nm\n$256\n", &payload, b"\n"])
    );
} // end fn every_byte_value_is_sent_unchanged

#[test]
fn only_the_given_payload_slice_is_sent() {
    let mut storage = [99, 0, 255, 99];
    let encoded = bytes(encode_publish("s", None, &storage[1..3]));
    storage.fill(42);

    assert_eq!(encoded, join(&[b"@PUB\n$1\ns\n$-1\n$2\n", &[0, 255, 10]]));
} // end fn only_the_given_payload_slice_is_sent

#[test]
fn invalid_segment_ids_are_refused() {
    let empty = "Invalid command. segment_id: Must not be empty.";
    let line_break = "Invalid command. segment_id: Must not contain CR or LF.";

    for command in [
        SegmentCommand::Subscribe,
        SegmentCommand::Unsubscribe,
        SegmentCommand::PresenceSubscribe,
        SegmentCommand::PresenceUnsubscribe,
    ] {
        assert_eq!(encode_segment_command(command, ""), configuration(empty));

        for segment_id in ["a\n", "a\r", "\r\n"] {
            assert_eq!(
                encode_segment_command(command, segment_id),
                configuration(line_break)
            );
        }
    }

    assert_eq!(encode_publish("", None, b"x"), configuration(empty));
    assert_eq!(encode_publish("a\n", None, b"x"), configuration(line_break));
    assert_eq!(encode_presence_list("", 1, 1, "1"), configuration(empty));

    assert_eq!(
        encode_presence_list("a\r", 1, 1, "1"),
        configuration(line_break)
    );
} // end fn invalid_segment_ids_are_refused

#[test]
fn invalid_message_ids_are_refused() {
    assert_eq!(
        encode_publish("s", Some(""), b"x"),
        configuration("Invalid command. message_id: Must not be empty.")
    );

    assert_eq!(
        encode_publish("s", Some("m\r"), b"x"),
        configuration("Invalid command. message_id: Must not contain CR or LF.")
    );

    assert_eq!(
        encode_publish("", Some("m\n"), b"x"),
        configuration(
            "Invalid command. segment_id: Must not be empty. message_id: Must not contain CR or LF."
        )
    );
} // end fn invalid_message_ids_are_refused

#[test]
fn presence_list_bounds_are_checked() {
    let page = "Invalid command. page: Must be at least 1.";
    let per_page_low = "Invalid command. per_page: Must be at least 1.";
    let per_page_high = "Invalid command. per_page: Must be at most 100.";

    for value in [0, -1, i32::MIN] {
        assert_eq!(
            encode_presence_list("s", value, 1, "1"),
            configuration(page)
        );

        assert_eq!(
            encode_presence_list("s", 1, value, "1"),
            configuration(per_page_low)
        );
    }

    for value in [101, i32::MAX] {
        assert_eq!(
            encode_presence_list("s", 1, value, "1"),
            configuration(per_page_high)
        );
    }

    // The edges themselves are accepted.
    assert_eq!(
        bytes(encode_presence_list("s", 1, 100, "1")),
        b"@PRES_LIST\n$1\ns\n;1\n;100\n$1\n1\n"
    );

    assert_eq!(
        bytes(encode_presence_list("s", i32::MAX, 1, "1")),
        b"@PRES_LIST\n$1\ns\n;2147483647\n;1\n$1\n1\n"
    );
} // end fn presence_list_bounds_are_checked

#[test]
fn presence_list_names_every_failed_field() {
    assert_eq!(
        encode_presence_list("s", 1, 1, ""),
        configuration("Invalid command. request_id: Must not be empty.")
    );

    assert_eq!(
        encode_presence_list("s", 1, 1, "1\n"),
        configuration("Invalid command. request_id: Must not contain CR or LF.")
    );

    assert_eq!(
        encode_presence_list("", 0, 101, "\r"),
        configuration(
            "Invalid command. segment_id: Must not be empty. page: Must be at least 1. per_page: Must be at most 100. request_id: Must not contain CR or LF."
        )
    );
} // end fn presence_list_names_every_failed_field

#[test]
fn a_refusal_never_repeats_the_input() {
    let Err(error) = encode_publish("synthetic-secret\n", Some("other-secret\r"), b"") else {
        panic!("the command must be refused");
    };

    assert_eq!(
        error,
        CelerisError::Configuration(
            "Invalid command. segment_id: Must not contain CR or LF. message_id: Must not contain CR or LF."
                .to_owned()
        )
    );
    assert!(!error.to_string().contains("secret"));
} // end fn a_refusal_never_repeats_the_input

#[test]
fn publish_limit_counts_all_framing() {
    // "@PUB\n", "$1\ns\n", "$-1\n", "$2097128\n" and the last LF are 24 bytes.
    let at_limit = bytes(encode_publish("s", None, &vec![7; LIMIT - 24]));
    assert_eq!(at_limit.len(), LIMIT);
    assert_eq!(&at_limit[..19], b"@PUB\n$1\ns\n$-1\n$2097");
    assert_eq!(&at_limit[LIMIT - 2..], b"\x07\n");

    assert_eq!(
        encode_publish("s", None, &vec![7; LIMIT - 23]),
        configuration(TOO_LARGE)
    );

    // "@PUB\n$1\ns\n$1\nm\n$2097127\n" and the last LF are 25 bytes.
    assert_eq!(
        bytes(encode_publish("s", Some("m"), &vec![0; LIMIT - 25])).len(),
        LIMIT
    );

    assert_eq!(
        encode_publish("s", Some("m"), &vec![0; LIMIT - 24]),
        configuration(TOO_LARGE)
    );
} // end fn publish_limit_counts_all_framing

#[test]
fn segment_command_limit_counts_all_framing() {
    // The command line, "$", seven length digits, LF, then the id and LF.
    let framing = [
        (SegmentCommand::Subscribe, 15),
        (SegmentCommand::Unsubscribe, 17),
        (SegmentCommand::PresenceSubscribe, 20),
        (SegmentCommand::PresenceUnsubscribe, 22),
    ];

    for (command, overhead) in framing {
        let segment_id = "s".repeat(LIMIT - overhead);

        assert_eq!(
            bytes(encode_segment_command(command, &segment_id)).len(),
            LIMIT,
            "{command:?}"
        );

        assert_eq!(
            encode_segment_command(command, &format!("{segment_id}s")),
            configuration(TOO_LARGE),
            "{command:?}"
        );
    }

    // Half as many characters as the limit, but each one is two bytes.
    assert_eq!(
        encode_segment_command(SegmentCommand::Subscribe, &"é".repeat(LIMIT / 2)),
        configuration(TOO_LARGE)
    );
} // end fn segment_command_limit_counts_all_framing

#[test]
fn presence_list_limit_counts_all_framing() {
    // "@PRES_LIST\n$1\ns\n;1\n;25\n$2097119\n" and the last LF are 33 bytes.
    let request_id = "1".repeat(LIMIT - 33);

    assert_eq!(
        bytes(encode_presence_list("s", 1, 25, &request_id)).len(),
        LIMIT
    );

    assert_eq!(
        encode_presence_list("s", 1, 25, &format!("{request_id}1")),
        configuration(TOO_LARGE)
    );
} // end fn presence_list_limit_counts_all_framing

#[test]
fn a_refused_command_does_not_affect_the_next() {
    assert_eq!(
        encode_publish("s", None, &vec![0; LIMIT - 23]),
        configuration(TOO_LARGE)
    );

    assert_eq!(
        bytes(encode_segment_command(SegmentCommand::Subscribe, "chat")),
        b"@SUB\n$4\nchat\n"
    );
} // end fn a_refused_command_does_not_affect_the_next

#[test]
fn identifier_rule_is_exact() {
    assert_eq!(identifier_rule(""), Some("Must not be empty"));

    for identifier in ["\r", "\n", "a\r", "\na", "a\r\nb"] {
        assert_eq!(
            identifier_rule(identifier),
            Some("Must not contain CR or LF"),
            "{identifier:?}"
        );
    }

    // No length limit and no character class: any other text is accepted.
    for identifier in ["a", "a\0b", " ", "識-9", "c:é"] {
        assert_eq!(identifier_rule(identifier), None, "{identifier:?}");
    }

    assert_eq!(identifier_rule(&"a".repeat(256)), None);
} // end fn identifier_rule_is_exact
