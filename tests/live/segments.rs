//! Subscriptions and segments (catalogue sections 3 and 5). What arrives on a
//! segment is decided by the server's membership, never by a listener
//! (SEG-01).

use std::time::Duration;

use tokio::time::sleep;
use useceleris_client::{Channel, ChannelState, ServerErrorType};

use crate::support::channels::{
    confirm_quiet, connect, connected, open, options, pair, publish_and_await, publish_text,
    segment, signing_provider, subscribe, subscribe_presence, unique_channel_reference,
};
use crate::support::collect::{Recorder, assert_payloads, collect, collect_channel, tagged};
use crate::support::environment::websocket_url;
use crate::support::signer::Claims;
use crate::support::wait::{
    EVENT_WAIT, await_channel_body, await_denial, await_error, is_server_error, settle,
};

/// The connection ids that a presence list of `segment_id` shows.
async fn connections_in(channel: &Channel, segment_id: &str) -> Vec<String> {
    let page = segment(channel, segment_id)
        .presence_list(1, 25)
        .await
        .expect("the presence list");

    page.connections
        .into_iter()
        .map(|connection| connection.connection_id)
        .collect()
} // end fn connections_in

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_nothing_to_a_listener_without_a_subscription() {
    let (publisher, receiver) = pair("listener-only", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));

    publish_text(&publisher, "chat", "unheard").await;
    confirm_quiet(&publisher, &receiver).await;

    assert_payloads(&chat, &[]);
} // end fn delivers_nothing_to_a_listener_without_a_subscription

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_after_a_subscription_made_before_connecting() {
    let reference = unique_channel_reference("before-connect");
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = open(
        options(websocket_url(), signing_provider(Claims::default())),
        &reference,
    );

    let chat = collect(&segment(&receiver, "chat"));
    subscribe(&segment(&receiver, "chat"));
    connect(&receiver).await;
    settle().await;

    publish_and_await(&publisher, &receiver, "chat", "hello").await;

    assert_payloads(&chat, &["hello"]);
} // end fn delivers_after_a_subscription_made_before_connecting

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_after_a_subscription_made_once_connected() {
    let (publisher, receiver) = pair("after-connect", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));
    subscribe(&segment(&receiver, "chat"));
    settle().await;

    publish_and_await(&publisher, &receiver, "chat", "hello").await;

    assert_payloads(&chat, &["hello"]);
} // end fn delivers_after_a_subscription_made_once_connected

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_to_a_listener_attached_after_subscribing() {
    let (publisher, receiver) = pair("listener-later", Claims::default()).await;
    subscribe(&segment(&receiver, "chat"));
    settle().await;
    let chat = collect(&segment(&receiver, "chat"));

    publish_and_await(&publisher, &receiver, "chat", "hello").await;

    assert_payloads(&chat, &["hello"]);
} // end fn delivers_to_a_listener_attached_after_subscribing

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stops_on_cancel_and_resumes_on_a_new_subscription() {
    let (publisher, receiver) = pair("rejoin", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));
    let first = subscribe(&segment(&receiver, "chat"));
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "one").await;

    first.cancel();
    settle().await;
    publish_text(&publisher, "chat", "two").await;
    confirm_quiet(&publisher, &receiver).await;
    assert_payloads(&chat, &["one"]);

    subscribe(&segment(&receiver, "chat"));
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "three").await;

    assert_payloads(&chat, &["one", "three"]);
} // end fn stops_on_cancel_and_resumes_on_a_new_subscription

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leaves_only_when_the_last_handle_cancels() {
    let (publisher, receiver) = pair("refcount", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));
    let first = subscribe(&segment(&receiver, "chat"));
    let second = subscribe(&segment(&receiver, "chat"));
    settle().await;

    first.cancel();
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "one").await;

    second.cancel();
    settle().await;
    publish_text(&publisher, "chat", "two").await;
    confirm_quiet(&publisher, &receiver).await;

    assert_payloads(&chat, &["one"]);
} // end fn leaves_only_when_the_last_handle_cancels

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_each_segments_messages_to_its_own_listeners_only() {
    let (publisher, receiver) = pair("demux", Claims::default()).await;
    let alpha = collect(&segment(&receiver, "alpha"));
    let beta = collect(&segment(&receiver, "beta"));
    subscribe(&segment(&receiver, "alpha"));
    let beta_membership = subscribe(&segment(&receiver, "beta"));
    settle().await;

    publish_and_await(&publisher, &receiver, "alpha", "a").await;
    publish_and_await(&publisher, &receiver, "beta", "b").await;
    assert_payloads(&alpha, &["a"]);
    assert_payloads(&beta, &["b"]);

    beta_membership.cancel();
    settle().await;
    publish_text(&publisher, "beta", "late").await;
    publish_and_await(&publisher, &receiver, "alpha", "still").await;
    sleep(Duration::from_millis(2500)).await;

    assert_payloads(&alpha, &["a", "still"]);
    assert_payloads(&beta, &["b"]);
} // end fn routes_each_segments_messages_to_its_own_listeners_only

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_other_listeners_and_the_subscription_when_one_listener_stops() {
    let (publisher, receiver) = pair("dispose", Claims::default()).await;
    let chat = segment(&receiver, "chat");
    let stopped = Recorder::default();
    let sink = stopped.clone();
    let stop_listening = chat.on_message(move |payload, _| sink.push(payload.to_vec()));
    let kept = collect(&chat);
    subscribe(&chat);
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "one").await;

    stop_listening.remove();
    publish_and_await(&publisher, &receiver, "chat", "two").await;

    assert_eq!(stopped.all(), [b"one".to_vec()]);
    assert_payloads(&kept, &["one", "two"]);
} // end fn keeps_other_listeners_and_the_subscription_when_one_listener_stops

/// A publish is not a membership: only a subscription receives (SEG-01).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_nothing_to_a_connection_that_only_publishes_and_delivers_once_it_subscribes() {
    let (publisher, receiver) = pair("publish-no-join", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));
    subscribe(&segment(&publisher, "chat"));
    settle().await;

    // The publish lands, which proves that it ran on the server.
    publish_and_await(&receiver, &publisher, "chat", "from-receiver").await;
    publish_text(&publisher, "chat", "unheard").await;
    confirm_quiet(&publisher, &receiver).await;
    assert_payloads(&chat, &[]);

    subscribe(&segment(&receiver, "chat"));
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "heard").await;

    assert_payloads(&chat, &["heard"]);
} // end fn delivers_nothing_to_a_connection_that_only_publishes_and_delivers_once_it_subscribes

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gives_no_echo_on_a_named_segment_before_a_subscription() {
    let reference = unique_channel_reference("publish-no-echo");
    let publisher = connected(&reference, Claims::default()).await;
    let echoing = connected(&reference, Claims::echo()).await;
    let chat = collect(&segment(&echoing, "chat"));

    publish_text(&echoing, "chat", "unheard").await;
    confirm_quiet(&publisher, &echoing).await;
    assert_payloads(&chat, &[]);

    subscribe(&segment(&echoing, "chat"));
    settle().await;
    publish_and_await(&echoing, &echoing, "chat", "echoed").await;

    assert_payloads(&chat, &["echoed"]);
} // end fn gives_no_echo_on_a_named_segment_before_a_subscription

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn does_not_list_a_connection_that_only_publishes_in_the_presence_of_the_segment() {
    let (publisher, receiver) = pair("publish-no-presence", Claims::default()).await;
    subscribe(&segment(&publisher, "chat"));
    settle().await;

    publish_and_await(&receiver, &publisher, "chat", "from-receiver").await;
    sleep(Duration::from_secs(3)).await;

    let listed = connections_in(&publisher, "chat").await;

    assert_eq!(listed.len(), 1, "chat {listed:?}, want one connection");
} // end fn does_not_list_a_connection_that_only_publishes_in_the_presence_of_the_segment

/// Watching presence is not membership: it neither joins nor holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_joins_or_holds_a_segment_for_a_presence_subscription() {
    let (publisher, receiver) = pair("presence-watch", Claims::default()).await;
    let chat = collect(&segment(&receiver, "chat"));
    subscribe_presence(&segment(&receiver, "chat"));
    settle().await;
    publish_text(&publisher, "chat", "unheard").await;
    confirm_quiet(&publisher, &receiver).await;
    assert_payloads(&chat, &[]);

    let messages = subscribe(&segment(&receiver, "chat"));
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "one").await;

    messages.cancel();
    settle().await;
    publish_text(&publisher, "chat", "two").await;
    confirm_quiet(&publisher, &receiver).await;

    assert_payloads(&chat, &["one"]);
} // end fn never_joins_or_holds_a_segment_for_a_presence_subscription

/// Write access publishes and read access subscribes. The two are separate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_write_only_tokens_subscription_and_still_delivers_its_publish() {
    let (publisher, receiver) = pair("publish-write-only", Claims::permission(false, true)).await;
    let chat = collect(&segment(&receiver, "chat"));
    subscribe(&segment(&publisher, "chat"));
    settle().await;

    let denied = await_denial(&receiver, "SUB", "chat");
    subscribe(&segment(&receiver, "chat"));
    denied.wait().await;

    publish_and_await(&receiver, &publisher, "chat", "written").await;
    publish_text(&publisher, "chat", "unheard").await;
    sleep(Duration::from_millis(2500)).await;

    assert_payloads(&chat, &[]);
    assert_eq!(receiver.state(), ChannelState::Connected);
} // end fn refuses_a_write_only_tokens_subscription_and_still_delivers_its_publish

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_read_only_tokens_publish_and_delivers_once_subscribed() {
    let (publisher, receiver) = pair("publish-read-only", Claims::permission(true, false)).await;
    let chat = collect(&segment(&receiver, "chat"));

    let denied = await_error(
        &receiver,
        |error| is_server_error(error, &ServerErrorType::PermissionDeniedError),
        "the publish denial",
        EVENT_WAIT,
    );

    publish_text(&receiver, "chat", "refused").await;
    denied.wait().await;
    publish_text(&publisher, "chat", "unheard").await;
    confirm_quiet(&publisher, &receiver).await;
    assert_payloads(&chat, &[]);

    subscribe(&segment(&receiver, "chat"));
    settle().await;
    publish_and_await(&publisher, &receiver, "chat", "heard").await;

    assert_payloads(&chat, &["heard"]);
} // end fn refuses_a_read_only_tokens_publish_and_delivers_once_subscribed

/// One channel is one WebSocket, and its segments share it (SEG-01).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiplexes_a_channels_segments_over_one_connection() {
    let reference = unique_channel_reference("multiplex");
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = connected(&reference, Claims::default()).await;
    subscribe(&segment(&receiver, "alpha"));
    subscribe(&segment(&receiver, "beta"));
    sleep(Duration::from_secs(3)).await;

    let alpha = connections_in(&publisher, "alpha").await;
    let beta = connections_in(&publisher, "beta").await;

    assert_eq!(alpha.len(), 1, "alpha {alpha:?}, want one connection");
    assert_eq!(beta, alpha, "beta {beta:?}, want the connection of alpha");

    let second = connected(&reference, Claims::default()).await;
    subscribe(&segment(&second, "alpha"));
    sleep(Duration::from_secs(3)).await;
    let both = connections_in(&publisher, "alpha").await;

    assert!(
        both.len() == 2 && both[0] != both[1],
        "alpha {both:?}, want two different connections"
    );
} // end fn multiplexes_a_channels_segments_over_one_connection

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn channel_listener_catches_deliveries_no_segment_listener_asked_for() {
    let (publisher, receiver) = pair("channel-listener", Claims::default()).await;
    let (seen, _registration) = collect_channel(&receiver);

    // A subscription with no segment listener, and the default segment.
    subscribe(&segment(&receiver, "unlistened"));
    settle().await;

    let last = await_channel_body(&receiver, "y");
    publish_text(&publisher, "unlistened", "x").await;
    publish_text(&publisher, "default", "y").await;
    last.wait().await;
    settle().await;

    let mut entries = tagged(&seen);
    entries.sort();

    assert_eq!(entries, ["default:y", "unlistened:x"]);
} // end fn channel_listener_catches_deliveries_no_segment_listener_asked_for

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removes_one_channel_listener_and_leaves_every_other_listener() {
    let (publisher, receiver) = pair("channel-listener-remove", Claims::default()).await;
    let (removed, removed_registration) = collect_channel(&receiver);
    let (kept, _kept_registration) = collect_channel(&receiver);
    let chat_segment = segment(&receiver, "chat");
    let chat = collect(&chat_segment);
    subscribe(&chat_segment);
    settle().await;

    // Channel listeners run after the segment listeners, so each step waits
    // for the last channel listener.
    let first = await_channel_body(&receiver, "one");
    publish_text(&publisher, "chat", "one").await;
    first.wait().await;

    removed_registration.remove();
    removed_registration.remove();
    let second = await_channel_body(&receiver, "two");
    publish_text(&publisher, "chat", "two").await;
    second.wait().await;

    assert_payloads(&removed, &["one"]);
    assert_payloads(&kept, &["one", "two"]);
    assert_payloads(&chat, &["one", "two"]);
} // end fn removes_one_channel_listener_and_leaves_every_other_listener
