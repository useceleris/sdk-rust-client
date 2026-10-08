//! A test-only signer, written from the protocol document. It writes the
//! token payload as the Go server signer does: the same key order, and the
//! string escapes of `JSON.stringify`. A claim that is `None` is left out,
//! as the reference live suites leave it out. The published crate never signs
//! (AUTH-05).

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha512;
use useceleris_client::Credentials;

use crate::support::environment::{client_id, signing_secret};

/// The channels that credentials may connect to.
#[derive(Clone, Debug)]
pub enum ChannelScope {
    /// Written as `null`: every channel.
    All,

    /// Written as a list. An empty list also permits every channel.
    Only(Vec<String>),
} // end enum ChannelScope

/// Read and write access, to every segment or to the listed segments.
#[derive(Clone, Debug)]
pub enum TokenPermission {
    All { read: bool, write: bool },
    Segments(Vec<SegmentPermission>),
} // end enum TokenPermission

#[derive(Clone, Debug)]
pub struct SegmentPermission {
    pub segment_id: String,
    pub read: bool,
    pub write: bool,
} // end struct SegmentPermission

/// The history replayed when a connection joins a segment.
#[derive(Clone, Copy, Debug)]
pub enum Replay {
    /// Written as `false`.
    Off,

    /// Written as `true`: everything the server still keeps.
    Backlog,

    /// Written as a number of milliseconds.
    Lookback(u64),
} // end enum Replay

/// The claims of one token. The default writes only a timestamp of now.
#[derive(Clone, Debug, Default)]
pub struct Claims {
    pub timestamp: Option<i64>,
    pub reference: Option<String>,
    pub channel_references: Option<ChannelScope>,
    pub token_permission: Option<TokenPermission>,
    pub replay: Option<Replay>,
    pub allow_echo: Option<bool>,
} // end struct Claims

impl Claims {
    pub fn timestamp(timestamp: i64) -> Claims {
        Claims {
            timestamp: Some(timestamp),
            ..Claims::default()
        }
    } // end fn timestamp

    pub fn reference(reference: &str) -> Claims {
        Claims {
            reference: Some(reference.to_owned()),
            ..Claims::default()
        }
    } // end fn reference

    pub fn channels(references: &[&str]) -> Claims {
        let references = references.iter().map(|reference| reference.to_string());

        Claims {
            channel_references: Some(ChannelScope::Only(references.collect())),
            ..Claims::default()
        }
    } // end fn channels

    pub fn permission(read: bool, write: bool) -> Claims {
        Claims {
            token_permission: Some(TokenPermission::All { read, write }),
            ..Claims::default()
        }
    } // end fn permission

    pub fn replay(replay: Replay) -> Claims {
        Claims {
            replay: Some(replay),
            ..Claims::default()
        }
    } // end fn replay

    pub fn echo() -> Claims {
        Claims {
            allow_echo: Some(true),
            ..Claims::default()
        }
    } // end fn echo
} // end impl Claims

pub fn now_millis() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");

    i64::try_from(since_epoch.as_millis()).expect("the time fits in 64 bits")
} // end fn now_millis

/// Signs `claims` for the configured client.
pub fn sign(claims: &Claims) -> Credentials {
    sign_with(client_id(), signing_secret(), claims)
} // end fn sign

/// Signs `claims` with any client id and secret, for refused credentials.
pub fn sign_with(client_id: &str, signing_secret: &str, claims: &Claims) -> Credentials {
    sign_raw(client_id, signing_secret, &payload_text(claims))
} // end fn sign_with

/// Signs any payload text as it is, for claims that [`Claims`] cannot show:
/// malformed JSON, or a missing or wrongly typed field.
pub fn sign_raw(client_id: &str, signing_secret: &str, payload_text: &str) -> Credentials {
    let payload = STANDARD.encode(payload_text.as_bytes());
    let mut digest =
        Hmac::<Sha512>::new_from_slice(signing_secret.as_bytes()).expect("HMAC takes any key");

    // The digest covers the encoded payload text, as the server checks it.
    digest.update(payload.as_bytes());
    let hexadecimal: String = digest
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    Credentials {
        payload,
        signature: STANDARD.encode(format!("{client_id}:{hexadecimal}")),
    }
} // end fn sign_raw

/// Writes the token JSON in the wire's key order: timestamp, reference,
/// channel_references, token_permission, replay, allow_echo.
fn payload_text(claims: &Claims) -> String {
    let timestamp = claims.timestamp.unwrap_or_else(now_millis);
    let mut text = format!("{{\"timestamp\":{timestamp}");

    if let Some(reference) = &claims.reference {
        text.push_str(",\"reference\":");
        push_json_string(&mut text, reference);
    }

    match &claims.channel_references {
        None => {}
        Some(ChannelScope::All) => text.push_str(",\"channel_references\":null"),
        Some(ChannelScope::Only(references)) => {
            text.push_str(",\"channel_references\":[");

            for (index, reference) in references.iter().enumerate() {
                if index > 0 {
                    text.push(',');
                }

                push_json_string(&mut text, reference);
            }

            text.push(']');
        }
    }

    match &claims.token_permission {
        None => {}
        Some(TokenPermission::All { read, write }) => {
            text.push_str(&format!(
                ",\"token_permission\":{{\"read\":{read},\"write\":{write}}}"
            ));
        }
        Some(TokenPermission::Segments(segments)) => {
            text.push_str(",\"token_permission\":[");

            for (index, segment) in segments.iter().enumerate() {
                if index > 0 {
                    text.push(',');
                }

                text.push_str("{\"segment_id\":");
                push_json_string(&mut text, &segment.segment_id);
                text.push_str(&format!(
                    ",\"read\":{},\"write\":{}}}",
                    segment.read, segment.write
                ));
            }

            text.push(']');
        }
    }

    match claims.replay {
        None => {}
        Some(Replay::Off) => text.push_str(",\"replay\":false"),
        Some(Replay::Backlog) => text.push_str(",\"replay\":true"),
        Some(Replay::Lookback(milliseconds)) => {
            text.push_str(&format!(",\"replay\":{milliseconds}"));
        }
    }

    if let Some(allow_echo) = claims.allow_echo {
        text.push_str(&format!(",\"allow_echo\":{allow_echo}"));
    }

    text.push('}');
    text
} // end fn payload_text

/// Quotes text as `JSON.stringify` does: it escapes the quote, the backslash
/// and the control characters below U+0020, and writes all else as it is.
fn push_json_string(text: &mut String, value: &str) {
    text.push('"');

    for character in value.chars() {
        match character {
            '"' => text.push_str("\\\""),
            '\\' => text.push_str("\\\\"),
            '\u{8}' => text.push_str("\\b"),
            '\u{c}' => text.push_str("\\f"),
            '\n' => text.push_str("\\n"),
            '\r' => text.push_str("\\r"),
            '\t' => text.push_str("\\t"),
            control if u32::from(control) < 0x20 => {
                text.push_str(&format!("\\u{:04x}", u32::from(control)));
            }
            other => text.push(other),
        }
    }

    text.push('"');
} // end fn push_json_string
