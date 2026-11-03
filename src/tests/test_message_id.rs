//! Message ids (RESEND-01): the ids that the SDK generates, and your own.

use std::collections::HashSet;

use futures_util::FutureExt;

use crate::constants::{SDK_LANGUAGE, SDK_VERSION};
use crate::errors::CelerisError;
use crate::random::{generate_message_id, message_id_prefix};
use crate::tests::support::connected_channel;

fn is_lowercase_hex(text: &str) -> bool {
    text.bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
} // end fn is_lowercase_hex

#[test]
fn generated_ids_name_the_sdk_and_its_version_then_32_hex_digits() {
    let message_id = generate_message_id();

    let random = message_id
        .strip_prefix(message_id_prefix("rust", SDK_VERSION).as_str())
        .expect("the id starts with the SDK's prefix");

    assert_eq!(random.len(), 32, "{message_id}");
    assert!(is_lowercase_hex(random), "{message_id}");
} // end fn generated_ids_name_the_sdk_and_its_version_then_32_hex_digits

#[test]
fn generated_ids_hold_only_letters_digits_dots_and_underscores() {
    for _ in 0..100 {
        let message_id = generate_message_id();

        assert!(
            message_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_'),
            "{message_id}"
        );
    }
} // end fn generated_ids_hold_only_letters_digits_dots_and_underscores

#[test]
fn generated_ids_are_different_on_every_call() {
    let identifiers: HashSet<String> = (0..1_000).map(|_| generate_message_id()).collect();

    assert_eq!(identifiers.len(), 1_000);
} // end fn generated_ids_are_different_on_every_call

#[test]
fn the_version_is_the_crates() {
    assert_eq!(SDK_LANGUAGE, "rust");
    assert_eq!(SDK_VERSION, env!("CARGO_PKG_VERSION"));
} // end fn the_version_is_the_crates

#[test]
fn every_other_version_character_becomes_an_underscore() {
    assert_eq!(
        message_id_prefix("rust", "1.2.0-beta.1"),
        "msg__rust_v1.2.0_beta.1__"
    );
    assert_eq!(
        message_id_prefix("rust", "1.2.0+build/7"),
        "msg__rust_v1.2.0_build_7__"
    );
    assert_eq!(message_id_prefix("rust", "0.1.0"), "msg__rust_v0.1.0__");
} // end fn every_other_version_character_becomes_an_underscore

/// Client commands write identifiers as bulk strings: `$`, the length, LF, the
/// bytes, LF. The server reads them without a scan for the end.
#[tokio::test(start_paused = true)]
async fn a_generated_id_is_sent_as_a_bulk_string() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    let (published, frame) = tokio::join!(chat.publish(b"hi"), peer.next_command());
    published.unwrap();

    let prefix = message_id_prefix("rust", SDK_VERSION);

    let message_id = frame
        .strip_prefix(format!("@PUB\n$4\nchat\n${}\n", prefix.len() + 32).as_str())
        .and_then(|rest| rest.strip_suffix("\n$2\nhi\n"))
        .expect("the frame carries the id as a bulk string");

    assert!(message_id.starts_with(prefix.as_str()), "{frame:?}");
    assert!(is_lowercase_hex(&message_id[prefix.len()..]), "{frame:?}");

    channel.close().await;
} // end fn a_generated_id_is_sent_as_a_bulk_string

/// Your own id needs at least one character. It is sent exactly as given, as a
/// bulk string.
#[tokio::test(start_paused = true)]
async fn your_own_id_is_sent_unchanged_as_a_bulk_string_from_one_character() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.default_segment();

    let (published, frame) = tokio::join!(
        lobby.publish_with_message_id(b"x", "a"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$7\ndefault\n$1\na\n$1\nx\n");

    let (published, frame) = tokio::join!(
        lobby.publish_with_message_id(b"y", "order-1042:shipped é"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(
        frame,
        "@PUB\n$7\ndefault\n$21\norder-1042:shipped é\n$1\ny\n"
    );

    channel.close().await;
} // end fn your_own_id_is_sent_unchanged_as_a_bulk_string_from_one_character

#[tokio::test(start_paused = true)]
async fn your_own_id_is_refused_when_empty_or_with_a_line_break() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.default_segment();
    let line_break = "Invalid command. message_id: Must not contain CR or LF.";

    let refusals = [
        ("", "Invalid command. message_id: Must not be empty."),
        ("a\nb", line_break),
        ("a\rb", line_break),
    ];

    for (message_id, message) in refusals {
        assert_eq!(
            lobby
                .publish_with_message_id(b"x", message_id)
                .now_or_never(),
            Some(Err(CelerisError::Configuration(message.to_owned()))),
            "{message_id:?}"
        );
    }

    // Nothing was written; the control publish is the first frame.
    let (published, frame) = tokio::join!(
        lobby.publish_with_message_id(b"x", "control"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$7\ndefault\n$7\ncontrol\n$1\nx\n");

    channel.close().await;
} // end fn your_own_id_is_refused_when_empty_or_with_a_line_break
