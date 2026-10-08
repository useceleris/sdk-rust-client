//! Reads the target stack from the environment, or from the crate's `.env`.
//! A real environment variable takes precedence over the file. Missing
//! configuration fails the test; it never skips it. Secret values are never
//! printed.

use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use std::{env, fs};

const REQUIRED: [&str; 3] = [
    "CELERIS_WS_URL",
    "CELERIS_CLIENT_ID",
    "CELERIS_SIGNING_SECRET",
];

/// The target stack. It has no `Debug`, so the secret cannot reach a log.
struct Environment {
    websocket_url: String,
    client_id: String,
    signing_secret: String,
    peer_websocket_url: Option<String>,

    /// `host:port` of CELERIS_WS_URL.
    authority: String,

    /// The path of CELERIS_WS_URL, without a trailing slash.
    path: String,
} // end struct Environment

static ENVIRONMENT: OnceLock<Environment> = OnceLock::new();

fn environment() -> &'static Environment {
    ENVIRONMENT.get_or_init(load)
} // end fn environment

pub fn websocket_url() -> &'static str {
    &environment().websocket_url
} // end fn websocket_url

pub fn client_id() -> &'static str {
    &environment().client_id
} // end fn client_id

pub fn signing_secret() -> &'static str {
    &environment().signing_secret
} // end fn signing_secret

/// CELERIS_WS_URL_PEER, a gateway that routes to a different server node.
/// Two connections through one gateway can share a node, so a cross-node test
/// returns early without it.
pub fn peer_websocket_url() -> Option<&'static str> {
    let peer = environment().peer_websocket_url.as_deref();

    if peer.is_none() {
        eprintln!("CELERIS_WS_URL_PEER is not set, so a second node cannot be reached. Skipped.");
    }

    peer
} // end fn peer_websocket_url

/// `host:port` of CELERIS_WS_URL, for the dropping proxy.
pub fn target_authority() -> &'static str {
    &environment().authority
} // end fn target_authority

/// The path of CELERIS_WS_URL, for the dropping proxy.
pub fn target_path() -> &'static str {
    &environment().path
} // end fn target_path

fn load() -> Environment {
    let file = read_environment_file(&Path::new(env!("CARGO_MANIFEST_DIR")).join(".env"));

    let lookup = |name: &str| {
        env::var(name)
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| file.get(name).cloned().filter(|value| !value.is_empty()))
    };

    let missing: Vec<&str> = REQUIRED
        .into_iter()
        .filter(|name| lookup(name).is_none())
        .collect();

    if !missing.is_empty() {
        panic!(
            "{} not set. Put CELERIS_WS_URL, CELERIS_CLIENT_ID and CELERIS_SIGNING_SECRET in the crate's .env (gitignored) or the environment.",
            missing.join(", ")
        );
    }

    let websocket_url = lookup("CELERIS_WS_URL").unwrap_or_default();
    let (authority, path) = split_url(&websocket_url);
    probe(&authority);

    Environment {
        client_id: lookup("CELERIS_CLIENT_ID").unwrap_or_default(),
        signing_secret: lookup("CELERIS_SIGNING_SECRET").unwrap_or_default(),
        peer_websocket_url: lookup("CELERIS_WS_URL_PEER"),
        websocket_url,
        authority,
        path,
    }
} // end fn load

/// Reads KEY=VALUE lines. A missing file is an empty one.
fn read_environment_file(path: &Path) -> HashMap<String, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
    };

    let mut values = HashMap::new();

    for line in text.lines().map(str::trim) {
        if line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let value = value
                .trim()
                .trim_matches(|character| character == '"' || character == '\'');
            values.insert(key.trim().to_owned(), value.to_owned());
        }
    }

    values
} // end fn read_environment_file

/// Splits `ws://host:port/path` into `host:port` and `/path`.
fn split_url(url: &str) -> (String, String) {
    let (rest, default_port) = if let Some(rest) = url.strip_prefix("ws://") {
        (rest, 80)
    } else if let Some(rest) = url.strip_prefix("wss://") {
        (rest, 443)
    } else {
        panic!("CELERIS_WS_URL must start with ws:// or wss://.");
    };

    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, path) = rest.split_at(end);
    let host_end = authority.rfind(']').map_or(0, |index| index + 1);
    let has_port = authority[host_end..].contains(':');

    let authority = if has_port {
        authority.to_owned()
    } else {
        format!("{authority}:{default_port}")
    };

    (authority, path.trim_end_matches('/').to_owned())
} // end fn split_url

/// Fails at once when the target cannot be reached, instead of letting every
/// test wait out its connect deadline.
fn probe(authority: &str) {
    let address = authority
        .to_socket_addrs()
        .ok()
        .and_then(|mut addresses| addresses.next());

    let reachable = address.is_some_and(|address| {
        TcpStream::connect_timeout(&address, Duration::from_secs(5)).is_ok()
    });

    assert!(
        reachable,
        "The realtime service at {authority} is not reachable. Start the stack, or point CELERIS_WS_URL elsewhere."
    );
} // end fn probe
