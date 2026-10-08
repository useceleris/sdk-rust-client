//! Pins the public surface: every public item, named once. A change to the
//! surface changes this file, so it is seen in review.

use std::fs;
use std::path::Path;

#[allow(unused_imports)]
use useceleris_client::{
    CelerisError, Channel, ChannelEventHandler, ChannelState, Client, ClientOptions,
    CredentialFuture, CredentialProvider, CredentialReason, CredentialRequest, Credentials,
    MessageMetadata, PayloadCodec, PresenceConnection, PresenceEvent, PresencePage, ProviderError,
    RecoveryEvent, Registration, Segment, ServerError, ServerErrorResource, ServerErrorType,
    ServerNotice, Subscription, read_text, text_payload,
};

/// The crate's public items, as `lib.rs` re-exports them.
const PUBLIC_ITEMS: [&str; 27] = [
    "CelerisError",
    "Channel",
    "ChannelEventHandler",
    "ChannelState",
    "Client",
    "ClientOptions",
    "CredentialFuture",
    "CredentialProvider",
    "CredentialReason",
    "CredentialRequest",
    "Credentials",
    "MessageMetadata",
    "PayloadCodec",
    "PresenceConnection",
    "PresenceEvent",
    "PresencePage",
    "ProviderError",
    "RecoveryEvent",
    "Registration",
    "Segment",
    "ServerError",
    "ServerErrorResource",
    "ServerErrorType",
    "ServerNotice",
    "Subscription",
    "read_text",
    "text_payload",
];

#[test]
fn the_public_surface_is_exactly_the_pinned_list() {
    let source = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
        .expect("lib.rs is readable");

    let mut exported: Vec<String> = source
        .split("pub use ")
        .skip(1)
        .flat_map(|statement| {
            let statement = statement.split(';').next().unwrap_or_default();
            let names = statement.rsplit("::").next().unwrap_or_default();

            names
                .trim_matches(|character: char| {
                    character == '{' || character == '}' || character.is_whitespace()
                })
                .split(',')
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>()
        })
        .collect();

    exported.sort();

    let mut expected: Vec<String> = PUBLIC_ITEMS.iter().map(|name| (*name).to_owned()).collect();
    expected.sort();

    assert_eq!(exported, expected);
} // end fn the_public_surface_is_exactly_the_pinned_list

/// The client never signs (AUTH-05): its runtime dependencies are exactly the
/// reviewed ones, and none of them is the server crate or a signing crate.
#[test]
fn the_runtime_dependencies_are_exactly_the_reviewed_ones() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("Cargo.toml is readable");

    let dependencies = manifest
        .split("[dependencies]")
        .nth(1)
        .and_then(|section| section.split("\n[").next())
        .expect("Cargo.toml has a dependencies section");

    let names: Vec<&str> = dependencies
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty())
        .collect();

    assert_eq!(
        names,
        [
            "futures-util",
            "getrandom",
            "rustls",
            "rustls-native-certs",
            "tokio",
            "tokio-tungstenite"
        ]
    );
} // end fn the_runtime_dependencies_are_exactly_the_reviewed_ones

/// No source file has a way to print or log, so nothing can leak a credential
/// to an output. Comments, which hold the documentation examples, are skipped.
#[test]
fn the_crate_prints_and_logs_nothing() {
    let source_directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for entry in fs::read_dir(&source_directory)
        .expect("src is readable")
        .flatten()
    {
        let path = entry.path();

        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }

        let source = fs::read_to_string(&path).expect("source files are readable");

        let code: String = source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        for forbidden in [
            "println!",
            "eprintln!",
            "print!(",
            "eprint!(",
            "dbg!(",
            "log::",
        ] {
            if code.contains(forbidden) {
                offenders.push(format!("{}: {forbidden}", path.display()));
            }
        }
    }

    assert!(offenders.is_empty(), "{offenders:?}");
} // end fn the_crate_prints_and_logs_nothing
