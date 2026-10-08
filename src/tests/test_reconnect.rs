//! Reconnection and recovery (catalogue section 9).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use tokio::runtime::Handle;
use tokio::time::{Instant, sleep};

use crate::channel::Channel;
use crate::constants::REPLAY_LOOKBACK_CAP;
use crate::credentials::{CredentialProvider, CredentialReason, CredentialRequest, Credentials};
use crate::errors::{CelerisError, protocol_error};
use crate::reconnect::replay_lookback;
use crate::tests::support::{
    DialBehaviour, Recorder, client_with_server, connect, connected_channel, delivery_frame,
    notice_frame, options_with_provider, presence_frame, presence_list_frame, publish_command,
    record_errors, record_states, segment_command, settle, test_credentials, timed_test_options,
    wait_until, yield_until,
};
use crate::values::{ChannelState, PresencePage, RecoveryEvent};

const HANDSHAKE_FAILED: &str = "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.";

/// One entry of a combined event log.
#[derive(Clone, Debug, PartialEq)]
enum Logged {
    State(ChannelState),
    Recovery(RecoveryEvent),
    Error(CelerisError),
} // end enum Logged

/// Records every later state change, recovery and error of `channel`.
async fn record_log(channel: &Channel) -> Recorder<Logged> {
    settle(channel).await;
    let log = Recorder::new();
    let states = log.clone();
    let recoveries = log.clone();
    let errors = log.clone();
    let events = channel.events();
    events.on_state_change(move |state| states.push(Logged::State(state)));
    events.on_recovery(move |event| recoveries.push(Logged::Recovery(event.clone())));
    events.on_error(move |error| errors.push(Logged::Error(error.clone())));

    log
} // end fn record_log

fn milliseconds(values: &[u64]) -> Vec<Duration> {
    values
        .iter()
        .map(|&value| Duration::from_millis(value))
        .collect()
} // end fn milliseconds

/// The gaps between `start` and each reconnect request, and between the
/// requests.
fn reconnect_gaps(requests: &[(Instant, CredentialRequest)], start: Instant) -> Vec<Duration> {
    let mut previous = start;
    let mut gaps = Vec::new();

    for (requested_at, request) in requests {
        if matches!(request.reason, CredentialReason::Reconnect { .. }) {
            gaps.push(*requested_at - previous);
            previous = *requested_at;
        }
    }

    gaps
} // end fn reconnect_gaps

fn reconnect_requests(requests: &[(Instant, CredentialRequest)]) -> Vec<CredentialRequest> {
    requests
        .iter()
        .filter(|(_, request)| matches!(request.reason, CredentialReason::Reconnect { .. }))
        .map(|(_, request)| request.clone())
        .collect()
} // end fn reconnect_requests

#[tokio::test(start_paused = true)]
async fn reconnect_delays_are_jittered_and_bounded_then_fail() {
    let (options, requests) = timed_test_options();
    let (client, server) = client_with_server(options, 0.5);
    let (channel, peer) = connect(&client, &server).await;
    let errors = record_errors(&channel).await;
    let states = record_states(&channel).await;

    // Every attempt is refused, so each fails as a transport error.
    server.set_behaviour(DialBehaviour::Refuse);
    let dropped_at = Instant::now();
    drop(peer);

    wait_until("the failed state", || {
        channel.state() == ChannelState::Failed
    })
    .await;
    settle(&channel).await;

    // Half of min(30 s, 500 ms × 2^retry index), for ten retries.
    let delays = milliseconds(&[
        250, 500, 1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000, 15_000,
    ]);
    assert_eq!(reconnect_gaps(&requests.all(), dropped_at), delays);
    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Failed]
    );
    assert_eq!(
        errors.all(),
        [CelerisError::Transport(HANDSHAKE_FAILED.to_owned())]
    );

    // A failed channel makes no more attempts.
    sleep(Duration::from_secs(60)).await;
    assert_eq!(requests.len(), 11);
    assert_eq!(server.urls.len(), 11);
} // end fn reconnect_delays_are_jittered_and_bounded_then_fail

#[tokio::test(start_paused = true)]
async fn retry_budget_resets_only_after_sixty_seconds_connected() {
    let (options, requests) = timed_test_options();
    let (client, server) = client_with_server(options, 0.5);
    let (channel, peer) = connect(&client, &server).await;

    // Three refused attempts, then the fourth, after 2 000 ms, connects.
    server.refuse_dials(3);
    let dropped_at = Instant::now();
    drop(peer);
    let peer = server.accept().await;
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    assert_eq!(
        reconnect_gaps(&requests.all(), dropped_at),
        milliseconds(&[250, 500, 1_000, 2_000])
    );

    // Connected for 59 999 ms: the budget stays used, so the next delay is
    // still the one for retry index 3.
    let answering = peer.answer_in_background();
    sleep(Duration::from_millis(59_999)).await;
    let dropped_at = Instant::now();
    drop(answering);
    let peer = server.accept().await;
    assert_eq!(Instant::now() - dropped_at, Duration::from_millis(2_000));
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;

    // Connected for 60 000 ms: the budget resets to retry index 0.
    let answering = peer.answer_in_background();
    sleep(Duration::from_millis(60_000)).await;
    let dropped_at = Instant::now();
    drop(answering);
    let _peer = server.accept().await;
    assert_eq!(Instant::now() - dropped_at, Duration::from_millis(250));
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
} // end fn retry_budget_resets_only_after_sixty_seconds_connected

#[tokio::test(start_paused = true)]
async fn recovery_follows_the_connected_state_with_its_retry_index() {
    let (channel, peer, server, _log) = connected_channel().await;
    let log = record_log(&channel).await;
    server.refuse_dials(2);
    drop(peer);

    let _peer = server.accept().await;
    wait_until("the recovery event", || log.len() == 3).await;
    settle(&channel).await;

    let recovery = RecoveryEvent {
        retry_index: 2,
        possible_gaps: true,
        possible_duplicates: true,
    };

    assert_eq!(
        log.all(),
        [
            Logged::State(ChannelState::Reconnecting),
            Logged::State(ChannelState::Connected),
            Logged::Recovery(recovery),
        ]
    );
} // end fn recovery_follows_the_connected_state_with_its_retry_index

#[tokio::test(start_paused = true)]
async fn reconnect_requests_fresh_credentials_with_a_growing_lookback() {
    let (options, requests) = timed_test_options();
    let (client, server) = client_with_server(options, 1.0);
    let (_channel, peer) = connect(&client, &server).await;
    sleep(Duration::from_secs(5)).await;

    // Attempts at 500, 1 500 and 3 500 ms are refused; the one at 7 500 ms
    // connects.
    server.refuse_dials(3);
    let wall_before = SystemTime::now();
    drop(peer);
    let _peer = server.accept().await;
    let wall_after = SystemTime::now();

    let reconnects = reconnect_requests(&requests.all());
    assert_eq!(reconnects.len(), 4);

    let mut lookbacks = Vec::new();
    let mut disconnections = Vec::new();

    for request in &reconnects {
        assert_eq!(request.channel_reference, "test-channel");

        let CredentialReason::Reconnect {
            disconnected_at,
            replay_lookback,
        } = request.reason
        else {
            unreachable!("only reconnect requests are kept");
        };

        lookbacks.push(replay_lookback);
        disconnections.push(disconnected_at);
    }

    // The outage so far plus five seconds of overlap.
    assert_eq!(lookbacks, milliseconds(&[5_500, 6_500, 8_500, 12_500]));

    // One disconnection time: when the socket dropped, on the wall clock.
    assert_eq!(disconnections, [disconnections[0]; 4]);
    assert!(wall_before <= disconnections[0] && disconnections[0] <= wall_after);
} // end fn reconnect_requests_fresh_credentials_with_a_growing_lookback

#[test]
fn replay_lookback_is_rounded_up_and_capped() {
    assert_eq!(
        replay_lookback(Duration::from_micros(1_500)),
        Duration::from_millis(5_002)
    );

    // The cap is the server's largest lookback, 4 294 967 295 ms.
    assert_eq!(REPLAY_LOOKBACK_CAP, Duration::from_millis(4_294_967_295));
    assert_eq!(
        replay_lookback(Duration::from_millis(4_294_962_294)),
        Duration::from_millis(4_294_967_294)
    );
    assert_eq!(
        replay_lookback(Duration::from_millis(4_294_962_295)),
        Duration::from_millis(4_294_967_295)
    );
    assert_eq!(
        replay_lookback(Duration::from_millis(4_294_962_296)),
        Duration::from_millis(4_294_967_295)
    );
    assert_eq!(
        replay_lookback(Duration::from_millis(5_000_000_000)),
        Duration::from_millis(4_294_967_295)
    );
} // end fn replay_lookback_is_rounded_up_and_capped

#[tokio::test(start_paused = true)]
async fn deterministic_reconnect_failure_is_terminal_at_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);

    // Valid credentials for the connect, then empty ones.
    let provider = CredentialProvider::new(move |_request| {
        let first = counter.fetch_add(1, Ordering::SeqCst) == 0;

        async move {
            if first {
                return Ok(test_credentials());
            }

            Ok(Credentials {
                payload: String::new(),
                signature: String::new(),
            })
        }
    });

    let (client, server) = client_with_server(options_with_provider(provider), 0.5);
    let (channel, peer) = connect(&client, &server).await;
    let log = record_log(&channel).await;
    drop(peer);

    wait_until("the failed state", || {
        channel.state() == ChannelState::Failed
    })
    .await;
    sleep(Duration::from_secs(60)).await;
    settle(&channel).await;

    let error = CelerisError::Configuration(
        "Invalid credentials. payload: Must not be empty. signature: Must not be empty.".to_owned(),
    );

    // The error comes before the failed state, and no attempt follows.
    assert_eq!(
        log.all(),
        [
            Logged::State(ChannelState::Reconnecting),
            Logged::Error(error),
            Logged::State(ChannelState::Failed),
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(server.urls.len(), 1);
} // end fn deterministic_reconnect_failure_is_terminal_at_once

#[tokio::test(start_paused = true)]
async fn undecodable_message_never_starts_a_reconnect() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let errors = record_errors(&channel).await;
    let delivered = Recorder::new();
    let collector = delivered.clone();
    channel
        .default_segment()
        .on_message(move |_payload, metadata| collector.push(metadata.message_id.clone()));

    peer.send_text("text").await;

    // A control message after the bad one still arrives.
    peer.send(delivery_frame("alice", "default", "id-1", 1, b"a"))
        .await;
    wait_until("the control message", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(
        errors.all(),
        [protocol_error(
            "Expected a binary WebSocket message.",
            "Message",
            0
        )]
    );
    assert_eq!(channel.state(), ChannelState::Connected);
    assert_eq!(server.urls.len(), 1);
    assert!(!server.has_pending_connection());

    // The same socket still carries commands.
    let _subscription = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));
} // end fn undecodable_message_never_starts_a_reconnect

#[tokio::test(start_paused = true)]
async fn transport_error_with_a_close_after_it_reconnects_once() {
    let (channel, peer, server, _log) = connected_channel().await;
    let states = record_states(&channel).await;
    peer.break_with_garbage().await;

    let mut restored = server.accept().await;
    let _subscription = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "control")
    );
    settle(&channel).await;

    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(server.urls.len(), 2);
    assert!(!server.has_pending_connection());
} // end fn transport_error_with_a_close_after_it_reconnects_once

#[tokio::test(start_paused = true)]
async fn restoration_precedes_any_publish_made_on_connected() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let _subscription = chat.subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    settle(&channel).await;
    let log = Recorder::new();
    let states = log.clone();
    let handle = Handle::current();
    let publisher = chat.clone();

    // The listener publishes while it runs, before it records the state.
    channel.events().on_state_change(move |state| {
        if state == ChannelState::Connected {
            let result = handle.block_on(publisher.publish_with_message_id("x", "m-1"));
            states.push(format!("published {result:?}"));
        }

        states.push(state.to_string());
    });

    let recoveries = log.clone();
    channel
        .events()
        .on_recovery(move |_event| recoveries.push("recovery".to_owned()));
    drop(peer);

    let mut restored = server.accept().await;
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "chat")
    );
    assert_eq!(
        restored.next_command().await,
        publish_command("chat", "m-1", "x")
    );
    wait_until("the recovery event", || log.len() == 4).await;
    settle(&channel).await;

    assert_eq!(
        log.all(),
        ["reconnecting", "published Ok(())", "connected", "recovery"]
    );
} // end fn restoration_precedes_any_publish_made_on_connected

#[tokio::test(start_paused = true)]
async fn handshake_frames_follow_connected_and_recovery() {
    let (channel, peer, server, _log) = connected_channel().await;
    settle(&channel).await;
    let log = Recorder::new();
    let states = log.clone();
    let recoveries = log.clone();
    let notices = log.clone();
    let events = channel.events();
    events.on_state_change(move |state| states.push(state.to_string()));
    events.on_recovery(move |_event| recoveries.push("recovery".to_owned()));
    events.on_notice(move |_notice| notices.push("notice".to_owned()));
    drop(peer);

    // The new socket carries a greeting the moment it opens.
    let mut restored = server.accept().await;
    restored.send(notice_frame(1, "hello")).await;
    wait_until("the notice", || log.len() == 4).await;
    settle(&channel).await;

    assert_eq!(
        log.all(),
        ["reconnecting", "connected", "recovery", "notice"]
    );
} // end fn handshake_frames_follow_connected_and_recovery

#[tokio::test(start_paused = true)]
async fn restoration_sends_current_intent_messages_then_presence() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let beta = channel.segment("beta").unwrap();
    let alpha = channel.segment("alpha").unwrap();
    let _beta = beta.subscribe().unwrap();
    let _alpha = alpha.subscribe().unwrap();
    let gone = channel.segment("gone").unwrap().subscribe().unwrap();
    let _default = channel.default_segment().subscribe_presence().unwrap();
    let _alpha_presence = alpha.subscribe_presence().unwrap();
    let brief = channel
        .segment("brief")
        .unwrap()
        .subscribe_presence()
        .unwrap();

    let (published, _) = tokio::join!(beta.publish_with_message_id("x", "m-1"), async {
        for _ in 0..7 {
            peer.next_command().await;
        }
    });

    published.unwrap();

    server.set_behaviour(DialBehaviour::Hang);
    drop(peer);
    yield_until("the reconnecting state", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;
    gone.cancel();
    brief.cancel();

    // The hanging attempt times out after 15 s; the next one connects.
    server.set_behaviour(DialBehaviour::Accept);
    let mut restored = server.accept().await;

    // Interests only, messages before presence, in registration order,
    // without cancelled intent and without the publish.
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "beta")
    );
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "alpha")
    );
    assert_eq!(
        restored.next_command().await,
        segment_command("PRES_SUB", "default")
    );
    assert_eq!(
        restored.next_command().await,
        segment_command("PRES_SUB", "alpha")
    );

    // The restored presence subscription delivers again.
    let presence = Recorder::new();
    let collector = presence.clone();
    alpha.on_presence(move |event| collector.push(event.connection_id.clone()));
    restored
        .send(presence_frame("alpha", "bob", "connection-1", true, 1))
        .await;
    wait_until("the presence event", || presence.len() == 1).await;
    assert_eq!(presence.all(), ["connection-1"]);

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "control")
    );
} // end fn restoration_sends_current_intent_messages_then_presence

#[tokio::test(start_paused = true)]
async fn close_frame_from_the_server_reconnects_once() {
    let (channel, peer, server, _log) = connected_channel().await;
    let states = record_states(&channel).await;
    peer.close().await;

    let mut restored = server.accept().await;
    let _subscription = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "control")
    );
    settle(&channel).await;

    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(server.urls.len(), 2);
    assert!(!server.has_pending_connection());
} // end fn close_frame_from_the_server_reconnects_once

#[tokio::test(start_paused = true)]
async fn request_ids_are_never_reused_across_reconnects() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    let (first, frame) = tokio::join!(chat.presence_list(1, 25), async {
        let frame = peer.next_command().await;
        peer.send(presence_list_frame("chat", "1", [0, 25, 1, 1, 0], &[]))
            .await;

        frame
    });

    first.unwrap();
    assert_eq!(frame, "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n1\n");
    drop(peer);

    let mut restored = server.accept().await;
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;

    let (second, frame) = tokio::join!(chat.presence_list(1, 25), async {
        let frame = restored.next_command().await;
        restored
            .send(presence_list_frame("chat", "2", [0, 25, 1, 1, 0], &[]))
            .await;

        frame
    });

    assert_eq!(frame, "@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n2\n");

    let page = PresencePage {
        segment_id: "chat".to_owned(),
        total: 0,
        per_page: 25,
        current_page: 1,
        from: 1,
        to: 0,
        connections: Vec::new(),
    };

    assert_eq!(second, Ok(page));
} // end fn request_ids_are_never_reused_across_reconnects
