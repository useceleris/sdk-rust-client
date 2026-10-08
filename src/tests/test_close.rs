//! Close (LIFE-03, RES-05): idempotent, terminal, bounded by its budget, and
//! it leaves no task or socket behind.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::{Instant, sleep};
use tokio_tungstenite::tungstenite::Message;

use crate::channel::Channel;
use crate::constants::MAXIMUM_COMMAND_BYTES;
use crate::credentials::CredentialProvider;
use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, FakeServer, Peer, ProviderLog, client_with_server, options_with_provider,
    settle, stall_writer, state_recorder, test_credentials, test_options, wait_until,
};
use crate::values::ChannelState;

const CLOSED_BEFORE_SENT: &str = "Channel closed before the publish was sent.";

const ATTEMPT_CANCELLED: &str =
    "Connection attempt cancelled: the channel was closed or a newer attempt started.";

fn test_channel(random: f64) -> (Channel, Arc<FakeServer>, ProviderLog) {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, random);

    (client.channel("test-channel").unwrap(), server, log)
} // end fn test_channel

async fn connected(random: f64) -> (Channel, Peer, Arc<FakeServer>, ProviderLog) {
    let (channel, server, log) = test_channel(random);
    let (result, peer) = tokio::join!(channel.connect(), server.accept());
    result.unwrap();

    (channel, peer, server, log)
} // end fn connected

/// Reads what the peer receives until the client's side is gone. A frame that
/// the client did not finish ends the reading.
async fn remaining_frame_sizes(peer: &mut Peer) -> Vec<usize> {
    let mut sizes = Vec::new();

    while let Some(message) = peer.next_message().await {
        if let Message::Binary(data) = message {
            sizes.push(data.len());
        }
    }

    sizes
} // end fn remaining_frame_sizes

#[tokio::test(start_paused = true)]
async fn close_is_idempotent() {
    let (channel, _server, _log) = test_channel(0.0);
    let states = state_recorder(&channel);

    tokio::join!(
        channel.close(),
        channel.close(),
        channel.close(),
        channel.close(),
        channel.close()
    );
    channel.close().await;
    settle(&channel).await;

    assert_eq!(channel.state(), ChannelState::Closed);
    assert_eq!(
        states.all(),
        vec![ChannelState::Closing, ChannelState::Closed]
    );
} // end fn close_is_idempotent

#[tokio::test(start_paused = true)]
async fn close_closes_a_connected_channel_with_the_closing_handshake() {
    let (channel, mut peer, _server, _log) = connected(0.0).await;

    // A listener added now must not see the connect's states.
    settle(&channel).await;
    let states = state_recorder(&channel);

    channel.close().await;
    settle(&channel).await;

    assert_eq!(
        states.all(),
        vec![ChannelState::Closing, ChannelState::Closed]
    );
    assert!(matches!(peer.next_message().await, Some(Message::Close(_))));
    assert!(peer.next_message().await.is_none());
} // end fn close_closes_a_connected_channel_with_the_closing_handshake

#[tokio::test(start_paused = true)]
async fn close_works_from_every_non_terminal_state() {
    let (idle, _server, _log) = test_channel(0.0);
    idle.close().await;

    let (failed, server, _log) = test_channel(0.0);
    server.set_behaviour(DialBehaviour::Refuse);
    failed.connect().await.unwrap_err();
    assert_eq!(failed.state(), ChannelState::Failed);
    failed.close().await;

    let (connecting, server, _log) = test_channel(0.0);
    server.set_behaviour(DialBehaviour::Hang);
    let attempting = connecting.clone();
    let attempt = tokio::spawn(async move { attempting.connect().await });
    wait_until("the dial", || server.urls.len() == 1).await;
    assert_eq!(connecting.state(), ChannelState::Connecting);
    connecting.close().await;
    assert_eq!(
        attempt.await.unwrap(),
        Err(CelerisError::Cancelled(ATTEMPT_CANCELLED.to_owned()))
    );

    let (connected_channel, _peer, _server, _log) = connected(0.0).await;
    connected_channel.close().await;

    let (reconnecting, peer, server, _log) = connected(0.0).await;
    server.set_behaviour(DialBehaviour::Hang);
    drop(peer);
    wait_until("reconnecting", || {
        reconnecting.state() == ChannelState::Reconnecting
    })
    .await;
    reconnecting.close().await;

    for channel in [idle, failed, connecting, connected_channel, reconnecting] {
        assert_eq!(channel.state(), ChannelState::Closed);
    }

    sleep(Duration::from_millis(1)).await;
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
} // end fn close_works_from_every_non_terminal_state

#[tokio::test(start_paused = true)]
async fn close_finishes_within_its_budget_when_the_peer_never_answers() {
    let (channel, mut peer, _server, _log) = connected(0.0).await;

    // The peer stops reading, so the writer can neither flush nor close.
    let writing = stall_writer(&channel).await;
    let lobby = channel.default_segment();
    let waiting = tokio::spawn(async move { lobby.publish_with_message_id(b"y", "m-2").await });
    sleep(Duration::from_millis(1)).await;

    let start = Instant::now();
    let closing = channel.clone();
    let closed = tokio::spawn(async move { closing.close().await });

    sleep(Duration::from_millis(4_999)).await;
    assert_eq!(channel.state(), ChannelState::Closing);
    assert!(!closed.is_finished());

    closed.await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    assert_eq!(channel.state(), ChannelState::Closed);

    assert_eq!(
        writing.await.unwrap(),
        Err(CelerisError::DeliveryUnknown(
            "The connection dropped while the socket was writing the publish, so it may or may not have been sent."
                .to_owned()
        ))
    );
    assert_eq!(
        waiting.await.unwrap(),
        Err(CelerisError::Cancelled(CLOSED_BEFORE_SENT.to_owned()))
    );

    // Only the first command was written in full.
    assert_eq!(
        remaining_frame_sizes(&mut peer).await,
        vec![MAXIMUM_COMMAND_BYTES]
    );
} // end fn close_finishes_within_its_budget_when_the_peer_never_answers

/// Sets a flag when the provider's future is dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    } // end method drop
} // end impl Drop for DropFlag

#[tokio::test(start_paused = true)]
async fn close_while_connecting_stops_the_attempt_and_drops_late_credentials() {
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let called = Arc::new(AtomicBool::new(false));
    let call = Arc::clone(&called);

    let provider = CredentialProvider::new(move |_request| {
        call.store(true, Ordering::SeqCst);
        let guard = DropFlag(Arc::clone(&flag));

        async move {
            let _guard = guard;
            sleep(Duration::from_secs(1)).await;

            Ok(test_credentials())
        }
    });

    let (client, server) = client_with_server(options_with_provider(provider), 0.0);
    let channel = client.channel("test-channel").unwrap();
    let connecting = channel.clone();
    let attempt = tokio::spawn(async move { connecting.connect().await });

    wait_until("the credential request", || called.load(Ordering::SeqCst)).await;
    channel.close().await;

    assert_eq!(
        attempt.await.unwrap(),
        Err(CelerisError::Cancelled(ATTEMPT_CANCELLED.to_owned()))
    );
    assert_eq!(channel.state(), ChannelState::Closed);

    // The credentials would have come after 1 s; they open nothing.
    wait_until("the provider dropped", || dropped.load(Ordering::SeqCst)).await;
    sleep(Duration::from_secs(10)).await;
    assert_eq!(server.urls.len(), 0);
    assert!(!server.has_pending_connection());
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
} // end fn close_while_connecting_stops_the_attempt_and_drops_late_credentials

#[tokio::test(start_paused = true)]
async fn close_while_reconnecting_clears_the_retry_timer() {
    // The jitter is fixed at half, so the first retry waits 250 ms.
    let (channel, peer, server, log) = connected(0.5).await;
    server.set_behaviour(DialBehaviour::Refuse);
    drop(peer);

    wait_until("reconnecting", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;
    channel.close().await;
    assert_eq!(channel.state(), ChannelState::Closed);

    // The retry timer is gone before its 250 ms are up.
    sleep(Duration::from_millis(1)).await;
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);

    sleep(Duration::from_secs(3600)).await;
    assert_eq!(log.requests.len(), 1);
    assert_eq!(server.urls.len(), 1);
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
} // end fn close_while_reconnecting_clears_the_retry_timer

#[tokio::test(start_paused = true)]
async fn a_publish_while_closing_is_refused_and_writes_nothing() {
    let (channel, mut peer, _server, _log) = connected(0.0).await;
    let _writing = stall_writer(&channel).await;

    let closing = channel.clone();
    let closed = tokio::spawn(async move { closing.close().await });
    sleep(Duration::from_millis(1)).await;
    assert_eq!(channel.state(), ChannelState::Closing);

    assert_eq!(
        channel
            .default_segment()
            .publish_with_message_id(b"x", "late")
            .await,
        Err(CelerisError::NotConnected(
            "Channel is not connected; it is closing.".to_owned()
        ))
    );

    closed.await.unwrap();

    // Only the first stalling command arrived, never the late one.
    assert_eq!(
        remaining_frame_sizes(&mut peer).await,
        vec![MAXIMUM_COMMAND_BYTES]
    );
} // end fn a_publish_while_closing_is_refused_and_writes_nothing

#[tokio::test(start_paused = true)]
async fn publishes_still_queued_at_close_are_cancelled() {
    let (channel, mut peer, _server, _log) = connected(0.0).await;
    let _writing = stall_writer(&channel).await;

    let mut queued = Vec::new();

    for message_id in ["one", "two"] {
        let lobby = channel.default_segment();

        queued.push(tokio::spawn(async move {
            lobby.publish_with_message_id(b"x", message_id).await
        }));
    }

    sleep(Duration::from_millis(1)).await;
    assert!(queued.iter().all(|publish| !publish.is_finished()));

    channel.close().await;

    for publish in queued {
        assert_eq!(
            publish.await.unwrap(),
            Err(CelerisError::Cancelled(CLOSED_BEFORE_SENT.to_owned()))
        );
    }

    assert_eq!(
        remaining_frame_sizes(&mut peer).await,
        vec![MAXIMUM_COMMAND_BYTES]
    );
} // end fn publishes_still_queued_at_close_are_cancelled

#[tokio::test(start_paused = true)]
async fn fifty_connect_and_close_cycles_leave_no_task_or_socket() {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let mut channels = Vec::new();

    for cycle in 0..50 {
        let channel = client.channel("test-channel").unwrap();

        // Alternate between closing a connected channel and closing during
        // the handshake.
        if cycle % 2 == 0 {
            server.set_behaviour(DialBehaviour::Accept);
            let (result, mut peer) = tokio::join!(channel.connect(), server.accept());
            result.unwrap();
            channel.close().await;

            assert!(matches!(peer.next_message().await, Some(Message::Close(_))));
            assert!(peer.next_message().await.is_none(), "cycle {cycle}");
        } else {
            server.set_behaviour(DialBehaviour::Hang);
            let connecting = channel.clone();
            let attempt = tokio::spawn(async move { connecting.connect().await });
            wait_until("the dial", || server.urls.len() == cycle + 1).await;
            channel.close().await;

            assert_eq!(
                attempt.await.unwrap(),
                Err(CelerisError::Cancelled(ATTEMPT_CANCELLED.to_owned()))
            );
        }

        assert_eq!(channel.state(), ChannelState::Closed);

        // Every handle stays alive, so nothing stops only because it dropped.
        channels.push(channel);
    }

    sleep(Duration::from_millis(1)).await;
    assert_eq!(log.requests.len(), 50);
    assert_eq!(server.urls.len(), 50);
    assert!(!server.has_pending_connection());
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
    assert_eq!(channels.len(), 50);
} // end fn fifty_connect_and_close_cycles_leave_no_task_or_socket
