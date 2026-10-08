//! Heartbeat (catalogue section 11, HEARTBEAT-01): every connection pings
//! every 20 s, and a ping unanswered for 15 s of reading time fails the
//! connection. The peer answers pings only while the test reads it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::runtime::Handle;
use tokio::time::{Instant, advance, sleep};
use tokio_tungstenite::tungstenite::Message;

use crate::channel::Channel;
use crate::credentials::{CredentialReason, CredentialRequest};
use crate::tests::support::{
    Peer, client_with_server, connect, connected_channel, delivery_frame, record_states, settle,
    timed_test_options, yield_until,
};
use crate::values::ChannelState;

fn seconds(values: &[u64]) -> Vec<Duration> {
    values
        .iter()
        .map(|&value| Duration::from_secs(value))
        .collect()
} // end fn seconds

/// When the client pinged `peer`, as offsets from `start`.
fn ping_offsets(peer: &Peer, start: Instant) -> Vec<Duration> {
    peer.ping_times()
        .into_iter()
        .map(|pinged_at| pinged_at - start)
        .collect()
} // end fn ping_offsets

/// The disconnection time and the lookback of a reconnect request.
fn reconnect_reason(request: &CredentialRequest) -> (SystemTime, Duration) {
    match request.reason {
        CredentialReason::Reconnect {
            disconnected_at,
            replay_lookback,
        } => (disconnected_at, replay_lookback),
        CredentialReason::Initial => panic!("expected a reconnect request"),
    }
} // end fn reconnect_reason

#[tokio::test(start_paused = true)]
async fn connected_channel_pings_every_twenty_seconds_while_idle() {
    let (_channel, mut peer, _server, _log) = connected_channel().await;
    let connected_at = Instant::now();
    let mut received = Vec::new();

    while received.len() < 3 {
        match peer.next_message().await {
            Some(Message::Ping(_)) => received.push(Instant::now() - connected_at),
            Some(_) => {}
            None => panic!("the client closed the socket"),
        }
    }

    assert_eq!(received, seconds(&[20, 40, 60]));
    assert_eq!(ping_offsets(&peer, connected_at), seconds(&[20, 40, 60]));
} // end fn connected_channel_pings_every_twenty_seconds_while_idle

#[tokio::test(start_paused = true)]
async fn unanswered_ping_fails_an_idle_connection_after_fifteen_seconds_of_reading() {
    let (options, requests) = timed_test_options();
    let (client, server) = client_with_server(options, 0.0);
    let (channel, mut dead) = connect(&client, &server).await;
    let connected_at = Instant::now();
    let states = record_states(&channel).await;
    let wall_before = SystemTime::now();

    // Nothing reads the peer, so the ping at 20 s goes unanswered.
    let replacement = server.accept().await;
    let wall_after = SystemTime::now();
    assert_eq!(Instant::now() - connected_at, Duration::from_secs(35));
    let _answering = replacement.answer_in_background();
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;

    // The outage began when the server was last heard from: at connect.
    let (_, reconnect) = &requests.all()[1];
    let (disconnected_at, replay_lookback) = reconnect_reason(reconnect);
    assert_eq!(replay_lookback, Duration::from_secs(40));
    let outage = Duration::from_secs(35);
    assert!(wall_before - outage <= disconnected_at && disconnected_at <= wall_after - outage);

    // The replacement answers its pings, and the dead socket's heartbeat
    // ended with it.
    sleep(Duration::from_secs(120) - (Instant::now() - connected_at)).await;
    settle(&channel).await;
    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(channel.state(), ChannelState::Connected);
    assert_eq!(server.urls.len(), 2);
    assert!(!server.has_pending_connection());
    assert_eq!(ping_offsets(&dead, connected_at), seconds(&[20]));
    assert!(matches!(dead.next_message().await, Some(Message::Ping(_))));
    assert_eq!(dead.next_message().await, None);
} // end fn unanswered_ping_fails_an_idle_connection_after_fifteen_seconds_of_reading

/// The outage began when the server was last heard from, here the pong at
/// 20 s, so the replay lookback covers the 35 silent seconds before the
/// heartbeat noticed, plus the usual 5.
#[tokio::test(start_paused = true)]
async fn a_heartbeat_outage_starts_when_the_server_was_last_heard() {
    let (options, requests) = timed_test_options();
    let (client, server) = client_with_server(options, 0.0);
    let (_channel, mut peer) = connect(&client, &server).await;
    let connected_at = Instant::now();

    // Reading answers the ping at 20 s; then the peer stops reading.
    assert_eq!(
        peer.frames_within(Duration::from_secs(30)).await,
        Vec::<String>::new()
    );
    let wall_before = SystemTime::now();
    let _replacement = server.accept().await;
    let wall_after = SystemTime::now();
    assert_eq!(Instant::now() - connected_at, Duration::from_secs(55));

    let all = requests.all();
    assert_eq!(all.len(), 2);
    let (requested_at, reconnect) = &all[1];
    assert_eq!(*requested_at - connected_at, Duration::from_secs(55));

    let (disconnected_at, replay_lookback) = reconnect_reason(reconnect);
    assert_eq!(replay_lookback, Duration::from_secs(40));
    let silence = Duration::from_secs(35);
    assert!(wall_before - silence <= disconnected_at && disconnected_at <= wall_after - silence);
} // end fn a_heartbeat_outage_starts_when_the_server_was_last_heard

/// A server ping is heard from the server, like a pong: the count starts
/// afresh at the next ping.
#[tokio::test(start_paused = true)]
async fn a_server_ping_clears_the_unanswered_ping() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let connected_at = Instant::now();
    let states = record_states(&channel).await;
    sleep(Duration::from_secs(30)).await;
    peer.send_ping().await;

    // Nothing answers the ping at 40 s.
    let _replacement = server.accept().await;
    assert_eq!(Instant::now() - connected_at, Duration::from_secs(55));
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    settle(&channel).await;
    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
} // end fn a_server_ping_clears_the_unanswered_ping

#[tokio::test(start_paused = true)]
async fn a_received_message_clears_the_unanswered_ping() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let connected_at = Instant::now();
    let states = record_states(&channel).await;
    sleep(Duration::from_secs(30)).await;
    peer.send(delivery_frame("alice", "chat", "id-1", 1, b"x"))
        .await;
    sleep(Duration::from_secs(20)).await;
    peer.send(delivery_frame("alice", "chat", "id-2", 2, b"x"))
        .await;

    // Nothing answers the ping at 60 s.
    let _replacement = server.accept().await;
    assert_eq!(Instant::now() - connected_at, Duration::from_secs(75));
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    settle(&channel).await;
    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
} // end fn a_received_message_clears_the_unanswered_ping

/// Starts to block in a listener for "chat" messages with id "id-1", and
/// returns the sender that releases it.
async fn block_in_a_listener(channel: &Channel) -> (Sender<()>, Arc<AtomicBool>) {
    let (release, released) = std::sync::mpsc::channel::<()>();
    let released = Mutex::new(released);
    let entered = Arc::new(AtomicBool::new(false));
    let listener_entered = Arc::clone(&entered);

    channel
        .segment("chat")
        .unwrap()
        .on_message(move |_payload, metadata| {
            if metadata.message_id == "id-1" {
                listener_entered.store(true, Ordering::SeqCst);
                released.lock().unwrap().recv().unwrap();
            }
        });

    (release, entered)
} // end fn block_in_a_listener

/// The clock does not move by itself while a listener blocks, so the test
/// moves it one second at a time and lets every task run in between.
async fn advance_seconds(count: u64) {
    for _ in 0..count {
        advance(Duration::from_secs(1)).await;

        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }
} // end fn advance_seconds

/// The server stops answering, and a listener blocks while the reader holds
/// the next message: reading time stops while the listener holds delivery,
/// so the unanswered ping never counts, and the pings go on meanwhile.
#[tokio::test(start_paused = true)]
async fn a_listener_blocking_for_ninety_seconds_keeps_its_connection() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let connected_at = Instant::now();
    let states = record_states(&channel).await;
    let (release, entered) = block_in_a_listener(&channel).await;

    peer.send(delivery_frame("alice", "chat", "id-1", 1, b"x"))
        .await;
    peer.send(delivery_frame("alice", "chat", "id-2", 2, b"y"))
        .await;
    yield_until("the listener to block", || entered.load(Ordering::SeqCst)).await;

    for _ in 0..100 {
        tokio::task::yield_now().await;
    }

    advance_seconds(90).await;

    assert_eq!(Instant::now() - connected_at, Duration::from_secs(90));
    assert_eq!(
        ping_offsets(&peer, connected_at),
        seconds(&[20, 40, 60, 80])
    );
    assert_eq!(states.all(), Vec::<ChannelState>::new());
    assert_eq!(channel.state(), ChannelState::Connected);
    assert_eq!(server.urls.len(), 1);
    assert!(!server.has_pending_connection());

    // Reading resumes, and the ping sent at 20 s has all 15 s left.
    release.send(()).unwrap();
    let _replacement = server.accept().await;
    assert_eq!(Instant::now() - connected_at, Duration::from_secs(105));
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    settle(&channel).await;
    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
} // end fn a_listener_blocking_for_ninety_seconds_keeps_its_connection

/// With nothing more to read, the reader keeps reading while the listener
/// blocks, so the server's pongs arrive and keep the connection.
#[tokio::test(start_paused = true)]
async fn a_listener_blocking_for_ninety_seconds_with_an_answering_server_keeps_its_connection() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let states = record_states(&channel).await;
    let (release, entered) = block_in_a_listener(&channel).await;

    peer.send(delivery_frame("alice", "chat", "id-1", 1, b"x"))
        .await;
    yield_until("the listener to block", || entered.load(Ordering::SeqCst)).await;
    let answering = peer.answer_in_background();
    advance_seconds(90).await;
    release.send(()).unwrap();
    sleep(Duration::from_secs(60)).await;

    // The control: a drop that the test causes is the first and only one.
    drop(answering);
    let _replacement = server.accept().await;
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    settle(&channel).await;

    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(server.urls.len(), 2);
    assert!(!server.has_pending_connection());
} // end fn a_listener_blocking_for_ninety_seconds_with_an_answering_server_keeps_its_connection

/// Each heartbeat stops with its socket, and close leaves no task behind.
#[tokio::test(start_paused = true)]
async fn no_heartbeat_outlives_its_socket() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let connected_at = Instant::now();
    assert_eq!(
        peer.frames_within(Duration::from_secs(25)).await,
        Vec::<String>::new()
    );
    assert_eq!(ping_offsets(&peer, connected_at), seconds(&[20]));
    drop(peer);

    let mut replacement = server.accept().await;
    let reconnected_at = Instant::now();
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    assert_eq!(
        replacement.frames_within(Duration::from_secs(50)).await,
        Vec::<String>::new()
    );

    channel.close().await;
    sleep(Duration::from_secs(60)).await;
    settle(&channel).await;

    assert_eq!(
        ping_offsets(&replacement, reconnected_at),
        seconds(&[20, 40])
    );
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
} // end fn no_heartbeat_outlives_its_socket
