//! Credentials and authentication (catalogue section 2).

use useceleris_client::{
    CelerisError, ChannelState, CredentialProvider, CredentialReason, CredentialRequest,
    Credentials,
};

use crate::support::channels::{
    connect, connected, open, options, recording_provider, signing_provider,
    unique_channel_reference,
};
use crate::support::collect::Recorder;
use crate::support::environment::{client_id, signing_secret, websocket_url};
use crate::support::signer::{ChannelScope, Claims, now_millis, sign, sign_raw, sign_with};
use crate::support::wait::{EVENT_WAIT, eventually};

/// Connects with `credentials` and expects a Transport failure that leaves
/// the channel failed (DEV-02: a refused handshake is never labelled an
/// authorization failure).
async fn assert_refused(case: &str, credentials: Credentials) {
    let signature = credentials.signature.clone();
    let provider = CredentialProvider::new(move |_request| {
        let credentials = credentials.clone();

        async move { Ok(credentials) }
    });

    let channel = open(
        options(websocket_url(), provider),
        &unique_channel_reference("refused"),
    );

    let result = channel.connect().await;

    let Err(CelerisError::Transport(message)) = result else {
        panic!("{case}: got {result:?}, want a Transport failure");
    };

    assert_eq!(channel.state(), ChannelState::Failed, "{case}: the state");
    assert!(
        !message.contains(&signature),
        "{case}: the error leaks the credentials"
    );
} // end fn assert_refused

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connects_with_valid_credentials_and_receives_the_greetings() {
    let channel = open(
        options(websocket_url(), signing_provider(Claims::default())),
        &unique_channel_reference("auth"),
    );

    let notices: Recorder<String> = Recorder::default();
    let sink = notices.clone();

    channel.events().on_notice(move |notice| {
        sink.push(String::from_utf8_lossy(&notice.payload).into_owned());
    });

    connect(&channel).await;

    eventually(
        || notices.count() >= 2,
        "the connect and default-subscribe greetings",
        EVENT_WAIT,
    )
    .await;

    let joined = notices.all().join("\n");

    assert!(
        joined.contains("Successfully connected") && joined.contains("segment \"default\""),
        "notices {joined:?}"
    );
} // end fn connects_with_valid_credentials_and_receives_the_greetings

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_an_invalid_signature_and_an_unknown_client_as_transport_failures() {
    let cases = [
        (
            "an invalid signature",
            sign_with(client_id(), "wrong-secret", &Claims::default()),
        ),
        (
            "an unknown client",
            sign_with("no-such-client", signing_secret(), &Claims::default()),
        ),
    ];

    for (case, credentials) in cases {
        assert_refused(case, credentials).await;
    }
} // end fn refuses_an_invalid_signature_and_an_unknown_client_as_transport_failures

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_malformed_claims_as_transport_failures() {
    let cases = [
        ("an empty reference", sign(&Claims::reference(""))),
        (
            "a payload that is not JSON",
            sign_raw(client_id(), signing_secret(), "not json"),
        ),
        (
            "a payload without a timestamp",
            sign_raw(client_id(), signing_secret(), "{\"reference\":\"x\"}"),
        ),
        (
            "a timestamp that is a string",
            sign_raw(client_id(), signing_secret(), "{\"timestamp\":\"now\"}"),
        ),
        (
            "a timestamp 30 seconds in the future",
            sign(&Claims::timestamp(now_millis() + 30_000)),
        ),
    ];

    for (case, credentials) in cases {
        assert_refused(case, credentials).await;
    }
} // end fn refuses_malformed_claims_as_transport_failures

/// The server window is 60 s (Edge): 61 s and 59 minutes old are refused, and
/// 5 minutes in the future too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_timestamps_outside_the_sixty_second_window() {
    let cases = [
        ("61 seconds old", now_millis() - 61_000),
        ("59 minutes old", now_millis() - 59 * 60_000),
        ("5 minutes in the future", now_millis() + 5 * 60_000),
    ];

    for (case, timestamp) in cases {
        assert_refused(case, sign(&Claims::timestamp(timestamp))).await;
    }
} // end fn refuses_timestamps_outside_the_sixty_second_window

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepts_a_timestamp_inside_the_sixty_second_window() {
    let channel = connected(
        &unique_channel_reference("window"),
        Claims::timestamp(now_millis() - 30_000),
    )
    .await;

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn accepts_a_timestamp_inside_the_sixty_second_window

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_channel_outside_the_tokens_restriction() {
    assert_refused(
        "another channel",
        sign(&Claims::channels(&["some-other-channel"])),
    )
    .await;
} // end fn refuses_a_channel_outside_the_tokens_restriction

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepts_a_channel_inside_the_tokens_restriction() {
    let reference = unique_channel_reference("allowed");
    let channel = connected(&reference, Claims::channels(&[&reference])).await;

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn accepts_a_channel_inside_the_tokens_restriction

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepts_a_channel_that_is_one_of_several_in_the_restriction() {
    let reference = unique_channel_reference("several");
    let channel = connected(
        &reference,
        Claims::channels(&["some-other-channel", &reference]),
    )
    .await;

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn accepts_a_channel_that_is_one_of_several_in_the_restriction

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepts_an_empty_channel_restriction_which_permits_every_channel() {
    let channel = connected(&unique_channel_reference("any"), Claims::channels(&[])).await;

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn accepts_an_empty_channel_restriction_which_permits_every_channel

/// The production signer writes `null` for every channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepts_a_null_channel_restriction_which_permits_every_channel() {
    let claims = Claims {
        channel_references: Some(ChannelScope::All),
        ..Claims::default()
    };

    let channel = connected(&unique_channel_reference("null-scope"), claims).await;

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn accepts_a_null_channel_restriction_which_permits_every_channel

/// Credentials are requested fresh for every attempt (D-001).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_fresh_credentials_for_every_connect() {
    let (provider, requests) = recording_provider(|_| Claims::default());
    let mut references = Vec::new();

    for _ in 0..2 {
        let reference = unique_channel_reference("fresh");
        let channel = open(options(websocket_url(), provider.clone()), &reference);
        connect(&channel).await;
        channel.close().await;
        references.push(reference);
    }

    let expected: Vec<CredentialRequest> = references
        .into_iter()
        .map(|channel_reference| CredentialRequest {
            channel_reference,
            reason: CredentialReason::Initial,
        })
        .collect();

    assert_eq!(requests.all(), expected);
} // end fn requests_fresh_credentials_for_every_connect
