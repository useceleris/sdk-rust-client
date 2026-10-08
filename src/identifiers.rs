use crate::constants::MAXIMUM_CHANNEL_REFERENCE_LENGTH;

/// Returns the rule that an identifier sent on the wire breaks, or `None` when
/// it is well formed: nonempty and free of CR and LF. Rust text is always valid
/// UTF-8, so the bytes sent are exactly the text that the caller passed
/// (D-003).
pub(crate) fn identifier_rule(identifier: &str) -> Option<&'static str> {
    if identifier.is_empty() {
        return Some("Must not be empty");
    }

    if identifier.contains(['\r', '\n']) {
        return Some("Must not contain CR or LF");
    }

    None
} // end fn identifier_rule

/// Returns the rule that a channel reference breaks, or `None`.
pub(crate) fn channel_reference_rule(reference: &str) -> Option<&'static str> {
    if reference.is_empty() {
        return Some("Must not be empty");
    }

    if reference.len() > MAXIMUM_CHANNEL_REFERENCE_LENGTH {
        return Some("Must be at most 255 characters");
    }

    let allowed =
        |character: u8| character.is_ascii_alphanumeric() || character == b'-' || character == b'_';

    if !reference.bytes().all(allowed) {
        return Some("Must contain only ASCII letters, digits, hyphens (-) or underscores (_)");
    }

    None
} // end fn channel_reference_rule
