//! Publishes that wait in the command queue across a rate-limit pause or a
//! reconnect, and publishes taken back by dropping their future (catalogue
//! section 10, QUEUE-01).

use std::time::Duration;

use tokio::time::{Instant, sleep};

use crate::channel::Channel;
use crate::client::ClientOptions;
use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, FakeServer, Peer, PendingPublish, client_with_server, connect,
    connected_channel, poll_once, publish_command, publish_now, receive_rate_limit, record_errors,
    segment_command, start_publish, test_options, wait_until, yield_until,
};
use crate::values::ChannelState;

const HANDSHAKE_FAILED: &str = "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.";

/// Drops the connection while every dial hangs until its deadline, which
/// leaves the channel reconnecting.
async fn drop_unreachable(channel: &Channel, server: &FakeServer, peer: Peer) {
    server.set_behaviour(DialBehaviour::Hang);
    drop(peer);
    yield_until("the reconnecting state", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;
} // end fn drop_unreachable

/// Lets dials through: the hanging attempt times out after 15 s, and the next
/// one, with zero jitter, connects at once.
async fn reconnect_now(channel: &Channel, server: &FakeServer) -> Peer {
    server.set_behaviour(DialBehaviour::Accept);
    let peer = server.accept().await;
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;

    peer
} // end fn reconnect_now

async fn assert_pending(publish: &mut PendingPublish<'_>) {
    let outcome = poll_once(publish).await;
    assert!(outcome.is_none(), "the publish ended early: {outcome:?}");
} // end fn assert_pending

#[tokio::test(start_paused = true)]
async fn cancelling_a_queued_publish_withdraws_it() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let limited_at = receive_rate_limit(&channel, &mut peer).await;

    drop(start_publish(&chat, "m-1", "x").await);
    let control = start_publish(&chat, "m-2", "y").await;

    // The dropped publish would go first.
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-2", "y")
    );
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    control.await.unwrap();
} // end fn cancelling_a_queued_publish_withdraws_it

#[tokio::test(start_paused = true)]
async fn cancelling_a_publish_queued_while_reconnecting_withdraws_it() {
    let (channel, peer, server, _log) = connected_channel().await;
    let lobby = channel.default_segment();
    drop_unreachable(&channel, &server, peer).await;

    drop(start_publish(&lobby, "m-1", "x").await);
    let control = start_publish(&lobby, "m-2", "y").await;

    let mut next = reconnect_now(&channel, &server).await;
    assert_eq!(
        next.next_command().await,
        publish_command("default", "m-2", "y")
    );
    control.await.unwrap();
} // end fn cancelling_a_publish_queued_while_reconnecting_withdraws_it

#[tokio::test(start_paused = true)]
async fn publish_while_reconnecting_is_sent_after_the_reconnect() {
    let (channel, peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    drop_unreachable(&channel, &server, peer).await;

    let mut pending = start_publish(&chat, "m-1", "x").await;
    sleep(Duration::from_secs(14)).await;
    assert_pending(&mut pending).await;

    let mut next = reconnect_now(&channel, &server).await;
    assert_eq!(
        next.next_command().await,
        publish_command("chat", "m-1", "x")
    );
    pending.await.unwrap();
} // end fn publish_while_reconnecting_is_sent_after_the_reconnect

/// The publishes that the writer never started go back to the front of the
/// queue, so they and those that waited for room behind them are sent after
/// the reconnect, in call order. Only the one being written may have been
/// sent: it is DeliveryUnknown and never sent again.
#[tokio::test(start_paused = true)]
async fn unwritten_publishes_are_sent_after_the_reconnect_in_order() {
    let (channel, peer, server, _log) = connected_channel().await;
    let lobby = channel.default_segment();
    let message_ids: Vec<String> = (0..66).map(|index| format!("m-{index}")).collect();
    peer.hold_writes();

    // 64 fill the writer, and two more wait for room.
    let mut publishes = Vec::new();

    for message_id in &message_ids {
        publishes.push(start_publish(&lobby, message_id, "x").await);
    }

    yield_until("the held write", || peer.has_held_write()).await;
    drop_unreachable(&channel, &server, peer).await;

    let mut publishes = publishes.into_iter();
    let first = publishes.next().unwrap();
    let mut waiting: Vec<PendingPublish<'_>> = publishes.collect();

    assert_eq!(
        first.await,
        Err(CelerisError::DeliveryUnknown(
            "The connection dropped while the socket was writing the publish, so it may or may not have been sent."
                .to_owned()
        ))
    );

    for publish in &mut waiting {
        assert_pending(publish).await;
    }

    let mut next = reconnect_now(&channel, &server).await;

    for message_id in &message_ids[1..] {
        assert_eq!(
            next.next_command().await,
            publish_command("default", message_id, "x")
        );
    }

    for publish in waiting {
        publish.await.unwrap();
    }
} // end fn unwritten_publishes_are_sent_after_the_reconnect_in_order

/// Restoration goes first, even ahead of a publish to a restored segment that
/// was queued before the drop; the publishes follow in call order.
#[tokio::test(start_paused = true)]
async fn restoration_precedes_publishes_queued_across_the_reconnect() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let alpha = channel.segment("alpha").unwrap();
    let beta = channel.segment("beta").unwrap();
    let _messages = alpha.subscribe().unwrap();
    let _presence = alpha.subscribe_presence().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "alpha"));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "alpha")
    );

    // The rate-limit pause holds the first publish in the queue.
    receive_rate_limit(&channel, &mut peer).await;
    let early = start_publish(&alpha, "q-1", "x").await;
    drop_unreachable(&channel, &server, peer).await;

    let late = start_publish(&beta, "q-2", "x").await;
    let later = start_publish(&alpha, "q-3", "x").await;

    let mut next = reconnect_now(&channel, &server).await;
    assert_eq!(next.next_command().await, segment_command("SUB", "alpha"));
    assert_eq!(
        next.next_command().await,
        segment_command("PRES_SUB", "alpha")
    );
    assert_eq!(
        next.next_command().await,
        publish_command("alpha", "q-1", "x")
    );
    assert_eq!(
        next.next_command().await,
        publish_command("beta", "q-2", "x")
    );
    assert_eq!(
        next.next_command().await,
        publish_command("alpha", "q-3", "x")
    );

    for publish in [early, late, later] {
        publish.await.unwrap();
    }
} // end fn restoration_precedes_publishes_queued_across_the_reconnect

/// A subscription cancelled while reconnecting still follows a publish to its
/// segment queued before it, so the segment ends up left as asked. A publish
/// never subscribes to presence, so a cancelled presence subscription needs
/// no command on the new socket.
#[tokio::test(start_paused = true)]
async fn cancelling_behind_a_queued_publish_leaves_the_segment_after_it() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let gamma = channel.segment("gamma").unwrap();
    let subscription = gamma.subscribe().unwrap();
    let presence = gamma.subscribe_presence().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "gamma"));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "gamma")
    );
    drop_unreachable(&channel, &server, peer).await;

    let queued = start_publish(&gamma, "g-1", "x").await;
    subscription.cancel();
    presence.cancel();

    let mut next = reconnect_now(&channel, &server).await;
    assert_eq!(
        next.next_command().await,
        publish_command("gamma", "g-1", "x")
    );
    assert_eq!(next.next_command().await, segment_command("UNSUB", "gamma"));
    queued.await.unwrap();

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(next.next_command().await, segment_command("SUB", "control"));
} // end fn cancelling_behind_a_queued_publish_leaves_the_segment_after_it

#[tokio::test(start_paused = true)]
async fn publish_while_reconnecting_is_refused_when_the_queue_is_full() {
    let (options, _log) = test_options();

    let options = ClientOptions {
        publish_queue_size: Some(1),
        ..options
    };

    let (client, server) = client_with_server(options, 0.0);
    let (channel, peer) = connect(&client, &server).await;
    let lobby = channel.default_segment();
    drop_unreachable(&channel, &server, peer).await;

    let waiting = start_publish(&lobby, "m-1", "x").await;

    assert_eq!(
        lobby.publish_with_message_id("x", "m-2").await,
        Err(CelerisError::Backpressure(
            "The publish queue is full (size 1). Retry once some publishes have gone out."
                .to_owned()
        ))
    );

    let mut next = reconnect_now(&channel, &server).await;
    assert_eq!(
        next.next_command().await,
        publish_command("default", "m-1", "x")
    );
    waiting.await.unwrap();
} // end fn publish_while_reconnecting_is_refused_when_the_queue_is_full

#[tokio::test(start_paused = true)]
async fn failed_reconnect_attempt_keeps_the_queue() {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, 0.5);
    let (channel, peer) = connect(&client, &server).await;
    let lobby = channel.default_segment();

    // The attempt at 250 ms is refused; the one 500 ms later connects.
    server.refuse_dials(1);
    let dropped_at = Instant::now();
    drop(peer);
    yield_until("the reconnecting state", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;

    let mut pending = start_publish(&lobby, "m-1", "x").await;
    sleep(Duration::from_millis(300)).await;

    // The connect, then the refused attempt.
    assert_eq!(log.requests.len(), 2);
    assert_eq!(channel.state(), ChannelState::Reconnecting);
    assert_pending(&mut pending).await;

    let mut next = server.accept().await;
    assert_eq!(Instant::now() - dropped_at, Duration::from_millis(750));
    assert_eq!(
        next.next_command().await,
        publish_command("default", "m-1", "x")
    );
    pending.await.unwrap();
} // end fn failed_reconnect_attempt_keeps_the_queue

/// Recovery that stops in failed refuses each queued publish with the error
/// that on_error reports, and a later connect starts with nothing queued.
#[tokio::test(start_paused = true)]
async fn queued_publishes_fail_with_the_terminal_error() {
    let (options, _log) = test_options();

    let options = ClientOptions {
        maximum_reconnect_attempts: Some(1),
        ..options
    };

    let (client, server) = client_with_server(options, 0.5);
    let (channel, peer) = connect(&client, &server).await;
    let lobby = channel.default_segment();
    let errors = record_errors(&channel).await;
    server.set_behaviour(DialBehaviour::Refuse);
    drop(peer);
    yield_until("the reconnecting state", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;

    let first = start_publish(&lobby, "m-1", "x").await;
    let second = start_publish(&lobby, "m-2", "x").await;
    wait_until("the failed state", || {
        channel.state() == ChannelState::Failed
    })
    .await;
    wait_until("the reported error", || errors.len() == 1).await;

    let terminal = CelerisError::Transport(HANDSHAKE_FAILED.to_owned());
    assert_eq!(errors.all(), std::slice::from_ref(&terminal));
    assert_eq!(first.await, Err(terminal.clone()));
    assert_eq!(second.await, Err(terminal));

    server.set_behaviour(DialBehaviour::Accept);
    let (connected, mut next) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    // A publish left in the queue would go first.
    publish_now(&lobby, &mut next, "m-3", "x").await;
} // end fn queued_publishes_fail_with_the_terminal_error

#[tokio::test(start_paused = true)]
async fn close_while_reconnecting_cancels_queued_publishes() {
    let (channel, peer, server, _log) = connected_channel().await;
    let lobby = channel.default_segment();
    drop_unreachable(&channel, &server, peer).await;

    let pending = start_publish(&lobby, "m-1", "x").await;
    channel.close().await;

    assert_eq!(
        pending.await,
        Err(CelerisError::Cancelled(
            "Channel closed before the publish was sent.".to_owned()
        ))
    );
} // end fn close_while_reconnecting_cancels_queued_publishes

/// A publish that waits out a rate-limit pause was never handed to the
/// socket, so it waits across the reconnect, which ends the pause.
#[tokio::test(start_paused = true)]
async fn publish_waiting_out_a_pause_is_sent_after_the_reconnect() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    receive_rate_limit(&channel, &mut peer).await;
    let pending = start_publish(&chat, "m-1", "x").await;
    drop(peer);

    let mut next = server.accept().await;
    assert_eq!(
        next.next_command().await,
        publish_command("chat", "m-1", "x")
    );
    pending.await.unwrap();
} // end fn publish_waiting_out_a_pause_is_sent_after_the_reconnect

/// A publish handed to a socket is never sent again after a reconnect, not
/// even the copy that a rate limit queued for resending.
#[tokio::test(start_paused = true)]
async fn rate_limit_resend_copy_is_not_sent_after_the_reconnect() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    publish_now(&chat, &mut peer, "m-1", "x").await;
    receive_rate_limit(&channel, &mut peer).await;
    drop(peer);

    let mut next = server.accept().await;
    yield_until("the connected state", || {
        channel.state() == ChannelState::Connected
    })
    .await;

    // The copy would go first.
    publish_now(&chat, &mut next, "m-2", "y").await;
} // end fn rate_limit_resend_copy_is_not_sent_after_the_reconnect

/// A publish taken back from the writer is never resent by a later rate
/// limit.
#[tokio::test(start_paused = true)]
async fn publish_taken_back_from_the_writer_is_never_resent() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    peer.hold_writes();
    let writing = start_publish(&chat, "m-1", "x").await;
    yield_until("the held write", || peer.has_held_write()).await;

    drop(start_publish(&chat, "m-2", "y").await);
    peer.release_writes();
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-1", "x")
    );
    writing.await.unwrap();

    receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-1", "x")
    );

    // The taken-back publish would go before this one.
    publish_now(&chat, &mut peer, "m-3", "z").await;
} // end fn publish_taken_back_from_the_writer_is_never_resent

/// A rate limit requeues a publish still waiting in the writer, so it has two
/// copies. Dropping its future takes back both: a cancelled publish never
/// goes out.
#[tokio::test(start_paused = true)]
async fn cancel_takes_back_every_copy_a_rate_limit_left() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    peer.hold_writes();
    let writing = start_publish(&chat, "m-0", "x").await;
    yield_until("the held write", || peer.has_held_write()).await;

    let waiting = start_publish(&chat, "m-1", "y").await;
    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    drop(waiting);
    peer.release_writes();
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-0", "x")
    );
    writing.await.unwrap();

    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-0", "x")
    );
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    publish_now(&chat, &mut peer, "m-2", "z").await;
} // end fn cancel_takes_back_every_copy_a_rate_limit_left

/// A publish dropped while the socket writes it may or may not be sent, and
/// is never resent, even by a rate limit that follows.
#[tokio::test(start_paused = true)]
async fn delivery_unknown_publish_is_never_resent() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    peer.hold_writes();
    let writing = start_publish(&chat, "m-1", "x").await;
    yield_until("the held write", || peer.has_held_write()).await;

    drop(writing);
    peer.release_writes();
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-1", "x")
    );

    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    let control = start_publish(&chat, "m-2", "y").await;

    // A resend would go before the control publish.
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-2", "y")
    );
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    control.await.unwrap();
} // end fn delivery_unknown_publish_is_never_resent
