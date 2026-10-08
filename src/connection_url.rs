use std::net::Ipv4Addr;

use tokio_tungstenite::tungstenite::http::Uri;

use crate::credentials::Credentials;
use crate::errors::CelerisError;

/// Accepts `wss://`, or `ws://` for a loopback host when the caller opted in
/// (ENDPOINT-01, SEC-02). Returns the URL without a trailing slash.
pub(crate) fn validate_base_url(
    base_url: &str,
    allow_insecure_loopback: bool,
) -> Result<String, CelerisError> {
    let invalid =
        |rule: &str| CelerisError::Configuration(format!("Invalid connection URL. {rule}"));

    if base_url.contains(['?', '#']) {
        return Err(invalid(
            "base_url must not contain a query string or fragment.",
        ));
    }

    let parsed: Uri = base_url
        .parse()
        .map_err(|_| invalid("base_url is not an absolute URL."))?;

    let (Some(scheme), Some(authority)) = (parsed.scheme_str(), parsed.authority()) else {
        return Err(invalid("base_url is not an absolute URL."));
    };

    if authority.as_str().contains('@') {
        return Err(invalid("base_url must not contain a username or password."));
    }

    let secure = scheme == "wss";
    let permitted_insecure =
        scheme == "ws" && allow_insecure_loopback && is_loopback(authority.host());

    if !secure && !permitted_insecure {
        return Err(invalid(
            "base_url must use wss://, or ws:// for a loopback host when allow_insecure_loopback is true.",
        ));
    }

    Ok(base_url.trim_end_matches('/').to_owned())
} // end fn validate_base_url

/// Accepts `localhost`, `::1` and dotted `127.x.x.x` addresses, as the
/// reference does.
fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") || host == "[::1]" {
        return true;
    }

    host.parse::<Ipv4Addr>()
        .is_ok_and(|address| address.octets()[0] == 127)
} // end fn is_loopback

/// Where one attempt connects. The credentials travel in its query, so it must
/// never reach an error, a log or a caller.
pub(crate) fn credential_url(
    base_url: &str,
    channel_reference: &str,
    credentials: &Credentials,
) -> String {
    format!(
        "{base_url}/channel/{channel_reference}?payload={}&signature={}",
        form_encode(&credentials.payload),
        form_encode(&credentials.signature),
    )
} // end fn credential_url

/// Encodes a query value as an HTML form does: unreserved characters stay,
/// a space becomes `+`, and every other byte becomes `%XX`.
fn form_encode(value: &str) -> String {
    const HEXADECIMAL: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(HEXADECIMAL[usize::from(byte >> 4)] as char);
                encoded.push(HEXADECIMAL[usize::from(byte & 0xF)] as char);
            }
        }
    }

    encoded
} // end fn form_encode
