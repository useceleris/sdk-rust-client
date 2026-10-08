//! The base URL rules, the credential URL and the channel reference rule
//! (ENDPOINT-01, SEC-02, AUTH-01).

use crate::connection_url::{credential_url, validate_base_url};
use crate::credentials::Credentials;
use crate::errors::CelerisError;
use crate::identifiers::channel_reference_rule;

const SCHEME: &str = "Invalid connection URL. base_url must use wss://, or ws:// for a loopback host when allow_insecure_loopback is true.";

const NOT_ABSOLUTE: &str = "Invalid connection URL. base_url is not an absolute URL.";

const USERINFO: &str = "Invalid connection URL. base_url must not contain a username or password.";

const QUERY: &str = "Invalid connection URL. base_url must not contain a query string or fragment.";

const CHARACTERS: &str = "Must contain only ASCII letters, digits, hyphens (-) or underscores (_)";

fn refused(message: &str) -> Result<String, CelerisError> {
    Err(CelerisError::Configuration(message.to_owned()))
} // end fn refused

fn credentials(payload: &str, signature: &str) -> Credentials {
    Credentials {
        payload: payload.to_owned(),
        signature: signature.to_owned(),
    }
} // end fn credentials

#[test]
fn secure_urls_are_accepted_without_a_trailing_slash() {
    for allow_insecure_loopback in [false, true] {
        let cases = [
            ("wss://example.test", "wss://example.test"),
            ("wss://example.test/", "wss://example.test"),
            (
                "wss://example.test:8443/prefix///",
                "wss://example.test:8443/prefix",
            ),
            ("wss://localhost", "wss://localhost"),
        ];

        for (base_url, expected) in cases {
            assert_eq!(
                validate_base_url(base_url, allow_insecure_loopback),
                Ok(expected.to_owned()),
                "{base_url}"
            );
        }
    }
} // end fn secure_urls_are_accepted_without_a_trailing_slash

#[test]
fn unsafe_urls_are_refused() {
    let cases = [
        ("ws://example.test", SCHEME),
        ("https://example.test", SCHEME),
        ("http://localhost", SCHEME),
        ("wss://user:pass@example.test", USERINFO),
        ("wss://user@example.test", USERINFO),
        ("wss://example.test?", QUERY),
        ("wss://example.test/?payload=x", QUERY),
        ("wss://example.test#", QUERY),
        ("not a url", NOT_ABSOLUTE),
        ("/channel", NOT_ABSOLUTE),
        ("example.test", NOT_ABSOLUTE),
        ("", NOT_ABSOLUTE),
    ];

    for (base_url, message) in cases {
        assert_eq!(
            validate_base_url(base_url, true),
            refused(message),
            "{base_url:?}"
        );
    }
} // end fn unsafe_urls_are_refused

#[test]
fn loopback_needs_the_explicit_opt_in() {
    for host in [
        "localhost",
        "LOCALHOST",
        "127.0.0.1",
        "127.1.2.3",
        "127.255.255.255",
        "[::1]",
    ] {
        for base_url in [format!("ws://{host}"), format!("ws://{host}:19002/")] {
            assert_eq!(
                validate_base_url(&base_url, false),
                refused(SCHEME),
                "{base_url}"
            );

            assert_eq!(
                validate_base_url(&base_url, true),
                Ok(base_url.trim_end_matches('/').to_owned()),
                "{base_url}"
            );
        }
    }
} // end fn loopback_needs_the_explicit_opt_in

#[test]
fn other_hosts_never_get_ws() {
    for host in [
        "localhost.evil.test",
        "evil-localhost",
        "128.0.0.1",
        "126.0.0.1",
        "0.0.0.0",
        "127.0.0.1.evil.test",
        "[::]",
        "[::2]",
        "[::ffff:127.0.0.1]",
    ] {
        let base_url = format!("ws://{host}");
        assert_eq!(
            validate_base_url(&base_url, true),
            refused(SCHEME),
            "{host}"
        );
    }
} // end fn other_hosts_never_get_ws

#[test]
fn credential_urls_keep_the_path_and_encode_each_value_once() {
    let reference = "a".repeat(255);

    for base_url in [
        "wss://example.test",
        "wss://example.test/",
        "wss://example.test/prefix///",
    ] {
        let base = validate_base_url(base_url, false).unwrap();
        let url = credential_url(&base, &reference, &credentials("+/%=&識", "%2B"));

        assert_eq!(
            url,
            format!("{base}/channel/{reference}?payload=%2B%2F%25%3D%26%E8%AD%98&signature=%252B")
        );
    }
} // end fn credential_urls_keep_the_path_and_encode_each_value_once

#[test]
fn credential_values_are_form_encoded() {
    let cases = [
        ("a b", "a+b"),
        ("AZaz09-._~", "AZaz09-._~"),
        ("eyJ0cyI6MX0=", "eyJ0cyI6MX0%3D"),
        ("a+b/c", "a%2Bb%2Fc"),
        ("é", "%C3%A9"),
        ("\n\0", "%0A%00"),
        ("", ""),
    ];

    for (value, encoded) in cases {
        assert_eq!(
            credential_url("wss://example.test", "room", &credentials(value, value)),
            format!("wss://example.test/channel/room?payload={encoded}&signature={encoded}"),
            "{value:?}"
        );
    }
} // end fn credential_values_are_form_encoded

#[test]
fn an_encoded_base_path_is_kept() {
    let base = validate_base_url("wss://example.test/a%2Fb/", false).unwrap();
    assert_eq!(base, "wss://example.test/a%2Fb");

    assert_eq!(
        credential_url(&base, "room", &credentials("p", "s")),
        "wss://example.test/a%2Fb/channel/room?payload=p&signature=s"
    );
} // end fn an_encoded_base_path_is_kept

#[test]
fn channel_reference_rule_is_exact() {
    assert_eq!(channel_reference_rule(""), Some("Must not be empty"));
    assert_eq!(channel_reference_rule("azAZ09-_"), None);
    assert_eq!(channel_reference_rule(&"a".repeat(255)), None);

    assert_eq!(
        channel_reference_rule(&"a".repeat(256)),
        Some("Must be at most 255 characters")
    );

    for character in ["`", "{", "@", "[", "/", ":", " ", ".", "\n", "é", "%"] {
        assert_eq!(
            channel_reference_rule(&format!("a{character}b")),
            Some(CHARACTERS),
            "{character:?}"
        );
    }
} // end fn channel_reference_rule_is_exact
