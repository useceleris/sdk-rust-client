use std::fmt::Write;

use crate::constants::{MESSAGE_ID_RANDOM_BYTES, SDK_LANGUAGE, SDK_VERSION};

/// Gives every publish an id, so that a resent copy is recognisable and
/// receivers drop it (RESEND-01). A generated id names the SDK that made it,
/// then holds 16 random bytes as hex:
/// `msg__rust_v0.1.0__0123456789abcdef0123456789abcdef`. The double
/// underscores keep it apart from the server's own `msg_{node}_` ids.
pub(crate) fn generate_message_id() -> String {
    let mut random = [0_u8; MESSAGE_ID_RANDOM_BYTES];
    fill_random(&mut random);

    random.iter().fold(
        message_id_prefix(SDK_LANGUAGE, SDK_VERSION),
        |mut text, byte| {
            // Writing to a String cannot fail.
            let _ = write!(text, "{byte:02x}");

            text
        },
    )
} // end fn generate_message_id

/// `msg__`, the language, `_v`, the version, `__`. An id holds only letters,
/// digits, dots and underscores, so any other character of the version, such
/// as the hyphen of a pre-release, becomes an underscore.
pub(crate) fn message_id_prefix(language: &str, version: &str) -> String {
    let version: String = version
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '.' {
                character
            } else {
                '_'
            }
        })
        .collect();

    format!("msg__{language}_v{version}__")
} // end fn message_id_prefix

/// A uniform value in `[0, 1)`, for the jitter of reconnect and rate-limit
/// delays.
pub(crate) fn random_fraction() -> f64 {
    let mut random = [0_u8; 8];
    fill_random(&mut random);

    // The top 53 bits fill an f64's mantissa exactly.
    (u64::from_le_bytes(random) >> 11) as f64 / (1_u64 << 53) as f64
} // end fn random_fraction

fn fill_random(buffer: &mut [u8]) {
    // The operating system's generator fails only when the platform has none,
    // and then no id this crate makes could be unique.
    getrandom::fill(buffer).expect("the operating system's random number generator is unavailable");
} // end fn fill_random
