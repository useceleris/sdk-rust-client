//! Payload helpers (HELP-01): the text helpers and the codec adapter. Rust's
//! standard library has no JSON, so JSON goes through the adapter.

use crate::errors::CelerisError;
use crate::payload::{PayloadCodec, read_text, text_payload};

const NOT_TEXT: &str = "Payload is not valid UTF-8, so it cannot be read as text.";

#[derive(Clone, Debug, PartialEq, Eq)]
struct Body {
    text: String,
} // end struct Body

#[derive(Debug, PartialEq, Eq)]
struct CallerFailure(&'static str);

#[test]
fn text_round_trips_byte_identically() {
    for (text, expected) in [
        ("", &b""[..]),
        ("hello", &b"hello"[..]),
        (
            "héllo 안녕 🛰️",
            &[
                0x68, 0xc3, 0xa9, 0x6c, 0x6c, 0x6f, 0x20, 0xec, 0x95, 0x88, 0xeb, 0x85, 0x95, 0x20,
                0xf0, 0x9f, 0x9b, 0xb0, 0xef, 0xb8, 0x8f,
            ][..],
        ),
    ] {
        let payload = text_payload(text);
        assert_eq!(payload, expected, "{text:?}");
        assert_eq!(read_text(&payload), Ok(text.to_owned()), "{text:?}");
    }
} // end fn text_round_trips_byte_identically

#[test]
fn read_text_refuses_invalid_utf8() {
    let invalid: [&[u8]; 6] = [
        &[0x80],
        &[0xc0, 0xaf],
        &[0xed, 0xa0, 0x80],
        &[0xf0, 0x9f, 0x98],
        &[0xf4, 0x90, 0x80, 0x80],
        &[0xff],
    ];

    for payload in invalid {
        assert_eq!(
            read_text(payload),
            Err(CelerisError::Configuration(NOT_TEXT.to_owned())),
            "{payload:?}"
        );
    }
} // end fn read_text_refuses_invalid_utf8

#[test]
fn read_text_never_repeats_the_payload() {
    let mut payload = b"synthetic-secret".to_vec();
    payload.push(0xff);

    let error = read_text(&payload).unwrap_err();
    assert_eq!(error, CelerisError::Configuration(NOT_TEXT.to_owned()));
    assert!(!error.to_string().contains("synthetic-secret"));
    assert!(!format!("{error:?}").contains("synthetic-secret"));
} // end fn read_text_never_repeats_the_payload

#[test]
fn a_codec_round_trips_through_the_callers_functions() {
    let codec = PayloadCodec::new(
        |body: &Body| Ok::<_, CallerFailure>(body.text.as_bytes().to_vec()),
        |payload: &[u8]| {
            Ok(Body {
                text: String::from_utf8_lossy(payload).into_owned(),
            })
        },
    );

    let body = Body {
        text: "héllo".to_owned(),
    };

    let payload = codec.encode_payload(&body).unwrap();
    assert_eq!(payload, "héllo".as_bytes());
    assert_eq!(codec.read_payload(&payload), Ok(body.clone()));

    // A clone uses the same functions.
    let copy = codec.clone();
    assert_eq!(copy.encode_payload(&body), Ok(payload));
    assert_eq!(format!("{copy:?}"), "PayloadCodec");
} // end fn a_codec_round_trips_through_the_callers_functions

#[test]
fn a_codec_returns_the_callers_failure_unchanged() {
    let codec = PayloadCodec::new(
        |_: &Body| Err::<Vec<u8>, _>(CallerFailure("synthetic-encoder-failure")),
        |_: &[u8]| Err::<Body, _>(CallerFailure("synthetic-decoder-failure")),
    );

    let body = Body {
        text: "x".to_owned(),
    };

    assert_eq!(
        codec.encode_payload(&body),
        Err(CallerFailure("synthetic-encoder-failure"))
    );

    assert_eq!(
        codec.read_payload(b""),
        Err(CallerFailure("synthetic-decoder-failure"))
    );
} // end fn a_codec_returns_the_callers_failure_unchanged
