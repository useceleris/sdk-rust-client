//! Heartbeat (catalogue section 11). HEARTBEAT-01: the server closes a
//! connection that it has heard no ping or pong from for 60 seconds. The
//! client sends its own pings and answers the server's.

use std::sync::mpsc;
use std::time::Duration;

use tokio::time::{Instant, sleep};
use useceleris_client::{ChannelState, CredentialReason};

use crate::support::channels::{
    connect, connected, open, options, publish_and_await, publish_text, recording_provider,
    segment, subscribe, unique_channel_reference,
};
use crate::support::collect::{Recorder, reconnect_requests};
use crate::support::proxy::DroppingProxy;
use crate::support::signer::Claims;
use crate::support::wait::{EVENT_WAIT, await_body, eventually, settle};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_an_idle_connection_open_past_the_servers_60_second_heartbeat() {
    let reference = unique_channel_reference("heartbeat-idle");
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = connected(&reference, Claims::default()).await;

    // Lets the connect's own state events reach their listeners first: a
    // listener registered at once could still receive them.
    settle().await;

    let states: Recorder<ChannelState> = Recorder::default();
    let sink = states.clone();
    receiver
        .events()
        .on_state_change(move |state| sink.push(state));
    subscribe(&segment(&receiver, "chat"));

    // The client answers the server's pings and sends its own. Nothing else
    // is sent.
    sleep(Duration::from_secs(95)).await;
    publish_and_await(&publisher, &receiver, "chat", "still-here").await;

    assert_eq!(states.all(), [], "the idle channel changed state");
    assert_eq!(receiver.state(), ChannelState::Connected);
} // end fn keeps_an_idle_connection_open_past_the_servers_60_second_heartbeat

/// A listener that holds delivery for 90 seconds keeps its connection: the
/// client pings on its own, and listeners never hold up the reader (DEV-01).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listener_blocking_ninety_seconds_keeps_its_connection() {
    let reference = unique_channel_reference("heartbeat");
    let blocking = connected(&reference, Claims::default()).await;
    let peer = connected(&reference, Claims::default()).await;
    let blocking_chat = segment(&blocking, "chat");
    let peer_chat = segment(&peer, "chat");
    subscribe(&blocking_chat);
    subscribe(&peer_chat);
    settle().await;

    let states: Recorder<ChannelState> = Recorder::default();
    let sink = states.clone();
    blocking
        .events()
        .on_state_change(move |state| sink.push(state));

    let (started_sender, started) = mpsc::channel();
    let (returned_sender, returned) = mpsc::channel();

    blocking_chat.on_message(move |payload, _| {
        if payload != b"block" {
            return;
        }

        let _ = started_sender.send(());
        std::thread::sleep(Duration::from_secs(90));
        let _ = returned_sender.send(());
    });

    settle().await;
    publish_text(&peer, "chat", "block").await;

    let started = tokio::task::spawn_blocking(move || started.recv_timeout(EVENT_WAIT));

    assert!(
        matches!(started.await, Ok(Ok(()))),
        "the blocking listener never ran"
    );

    let returned =
        tokio::task::spawn_blocking(move || returned.recv_timeout(Duration::from_secs(120)));

    assert!(
        matches!(returned.await, Ok(Ok(()))),
        "the blocking listener never returned"
    );

    // A close that the server sent while the listener ran is read now.
    sleep(Duration::from_secs(3)).await;

    assert_eq!(states.all(), [], "the blocking channel changed state");

    let after = await_body(&peer_chat, "after");
    publish_text(&blocking, "chat", "after").await;
    after.wait().await;

    assert_eq!(blocking.state(), ChannelState::Connected);
} // end fn a_listener_blocking_ninety_seconds_keeps_its_connection

/// A path that silently stops carrying anything, while TCP to the proxy
/// stays up, is found dead once a ping goes 15 seconds unanswered. The next
/// ping goes out within 20 seconds, so recovery starts 15 to 35 seconds after
/// the path died, and asks to replay the whole silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silently_dead_path_is_found_and_recovered() {
    let proxy = DroppingProxy::start().await;
    let (provider, requests) = recording_provider(|_| Claims::default());
    let channel = open(
        options(proxy.url(), provider),
        &unique_channel_reference("blackhole"),
    );

    connect(&channel).await;
    settle().await;

    let states: Recorder<(ChannelState, Duration)> = Recorder::default();
    let sink = states.clone();
    let died_at = Instant::now();

    channel
        .events()
        .on_state_change(move |state| sink.push((state, died_at.elapsed())));

    proxy.blackhole_all();

    eventually(
        || states.count() >= 2,
        "reconnecting, then connected",
        Duration::from_secs(45),
    )
    .await;

    let changes = states.all();

    assert_eq!(
        [changes[0].0, changes[1].0],
        [ChannelState::Reconnecting, ChannelState::Connected],
        "changes {changes:?}"
    );

    let found_dead_after = changes[0].1;

    // A ping in flight when the path died counts from when it was sent.
    assert!(
        found_dead_after >= Duration::from_millis(14_500),
        "found dead after {found_dead_after:?}, before a ping could go 15 s unanswered"
    );

    // The outage began when the server was last heard from, before the path
    // died, so the lookback covers the silence plus five seconds.
    let reconnects = reconnect_requests(&requests);
    let Some(CredentialReason::Reconnect {
        replay_lookback, ..
    }) = reconnects.first().map(|request| request.reason.clone())
    else {
        panic!("recovered without a reconnect credential request");
    };

    assert!(
        replay_lookback + Duration::from_secs(1) >= found_dead_after + Duration::from_secs(5),
        "replay lookback {replay_lookback:?} misses a silence of {found_dead_after:?}"
    );

    channel
        .default_segment()
        .publish(b"after")
        .await
        .expect("the publish after recovery is accepted");
} // end fn a_silently_dead_path_is_found_and_recovered
