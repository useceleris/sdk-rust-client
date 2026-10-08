//! Replay and deduplication (catalogue section 8). REV-01: a token's replay
//! claim makes each segment join first deliver that segment's recent history,
//! ids kept.

use std::time::Duration;

use tokio::time::sleep;

use crate::support::channels::{
    connect, connected, open, options, publish_all, publish_paced, segment, signing_provider,
    subscribe, unique_channel_reference,
};
use crate::support::collect::{
    Delivery, Recorder, assert_payloads, collect, collect_channel, distinct_message_ids,
    is_generated_message_id, numbered, tagged,
};
use crate::support::environment::websocket_url;
use crate::support::signer::{Claims, Replay};
use crate::support::wait::{await_body, await_channel_body, settle};

/// The server keeps at most this many messages per segment for replay.
const BACKLOG_CAPACITY: usize = 100;

/// Each delivery as its body and its message id.
fn identified(recorder: &Recorder<Delivery>) -> Vec<(String, String)> {
    let deliveries = recorder.all();

    deliveries
        .iter()
        .map(|delivery| (delivery.text(), delivery.metadata.message_id.clone()))
        .collect()
} // end fn identified

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_recent_messages_with_identical_ids() {
    let reference = unique_channel_reference("replay");
    let publisher = connected(&reference, Claims::default()).await;
    let live_receiver = connected(&reference, Claims::default()).await;
    let live_history = segment(&live_receiver, "history");
    let live = collect(&live_history);
    subscribe(&live_history);
    settle().await;

    let last_live = await_body(&live_history, "three");
    publish_all(&publisher, "history", &["one", "two", "three"]).await;
    last_live.wait().await;

    let replay_receiver = connected(&reference, Claims::replay(Replay::Lookback(60_000))).await;
    let replay_history = segment(&replay_receiver, "history");
    let replayed = collect(&replay_history);
    let last_replayed = await_body(&replay_history, "three");
    subscribe(&replay_history);
    last_replayed.wait().await;

    assert_eq!(identified(&replayed), identified(&live));
    assert_payloads(&replayed, &["one", "two", "three"]);

    for (_, message_id) in identified(&replayed) {
        assert!(
            is_generated_message_id(&message_id),
            "message id {message_id:?}"
        );
    }
} // end fn replays_recent_messages_with_identical_ids

/// No replay claim, and a `false` claim, both give no history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_nothing_to_a_token_without_a_replay_claim() {
    let reference = unique_channel_reference("no-replay");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "history", &["old-1", "old-2"]).await;
    settle().await;

    let absent = connected(&reference, Claims::default()).await;
    let off = connected(&reference, Claims::replay(Replay::Off)).await;
    let absent_history = segment(&absent, "history");
    let off_history = segment(&off, "history");
    let absent_delivered = collect(&absent_history);
    let off_delivered = collect(&off_history);
    subscribe(&absent_history);
    subscribe(&off_history);
    settle().await;

    // The live message proves the join, so a replay would have arrived first.
    let absent_live = await_body(&absent_history, "live");
    let off_live = await_body(&off_history, "live");
    publish_all(&publisher, "history", &["live"]).await;
    absent_live.wait().await;
    off_live.wait().await;
    settle().await;

    assert_payloads(&absent_delivered, &["live"]);
    assert_payloads(&off_delivered, &["live"]);
} // end fn replays_nothing_to_a_token_without_a_replay_claim

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_in_publish_order() {
    let reference = unique_channel_reference("replay-order");
    let publisher = connected(&reference, Claims::default()).await;
    let published = numbered("m", 10);
    let published: Vec<&str> = published.iter().map(String::as_str).collect();
    publish_all(&publisher, "history", &published).await;
    settle().await;

    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&receiver, "history");
    let delivered = collect(&history);
    let last = await_body(&history, "m9");
    subscribe(&history);
    last.wait().await;
    settle().await;

    assert_payloads(&delivered, &published);
} // end fn replays_in_publish_order

/// Edge: 105 published, the last 100 replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_at_most_the_last_hundred_messages_of_a_segment() {
    let reference = unique_channel_reference("replay-capacity");
    let publisher = connected(&reference, Claims::default()).await;
    let published = numbered("m", BACKLOG_CAPACITY + 5);
    let published: Vec<&str> = published.iter().map(String::as_str).collect();

    // Paced below the per-second publish limit, so no publish is resent.
    publish_paced(&publisher, "history", &published).await;

    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&receiver, "history");
    let delivered = collect(&history);
    let last = await_body(&history, published[published.len() - 1]);
    subscribe(&history);
    last.wait().await;
    settle().await;

    assert_payloads(&delivered, &published[published.len() - BACKLOG_CAPACITY..]);
} // end fn replays_at_most_the_last_hundred_messages_of_a_segment

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_only_the_window_of_a_numeric_replay_claim() {
    let reference = unique_channel_reference("replay-window");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "history", &["old"]).await;
    sleep(Duration::from_secs(5)).await;
    publish_all(&publisher, "history", &["recent"]).await;
    settle().await;

    let receiver = connected(&reference, Claims::replay(Replay::Lookback(3_000))).await;
    let history = segment(&receiver, "history");
    let delivered = collect(&history);
    let recent = await_body(&history, "recent");
    subscribe(&history);
    recent.wait().await;
    sleep(Duration::from_millis(2500)).await;

    assert_payloads(&delivered, &["recent"]);
} // end fn replays_only_the_window_of_a_numeric_replay_claim

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_the_default_segment_on_connect() {
    let reference = unique_channel_reference("replay-default");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "default", &["d1", "d2"]).await;
    settle().await;

    // The listener is in place before connect, when the server joins default.
    let receiver = open(
        options(
            websocket_url(),
            signing_provider(Claims::replay(Replay::Backlog)),
        ),
        &reference,
    );

    let lobby = collect(&receiver.default_segment());
    let last = await_body(&receiver.default_segment(), "d2");
    connect(&receiver).await;
    last.wait().await;

    assert_payloads(&lobby, &["d1", "d2"]);
} // end fn replays_the_default_segment_on_connect

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_each_segments_backlog_when_that_segment_is_joined() {
    let reference = unique_channel_reference("replay-per-join");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "alpha", &["a1"]).await;
    publish_all(&publisher, "beta", &["b1"]).await;
    settle().await;

    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let alpha_segment = segment(&receiver, "alpha");
    let beta_segment = segment(&receiver, "beta");
    let alpha = collect(&alpha_segment);
    let beta = collect(&beta_segment);
    let alpha_replay = await_body(&alpha_segment, "a1");
    subscribe(&alpha_segment);
    alpha_replay.wait().await;
    settle().await;
    assert_payloads(&beta, &[]);

    let beta_replay = await_body(&beta_segment, "b1");
    subscribe(&beta_segment);
    beta_replay.wait().await;

    assert_payloads(&alpha, &["a1"]);
    assert_payloads(&beta, &["b1"]);
} // end fn replays_each_segments_backlog_when_that_segment_is_joined

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_to_a_segment_joined_by_publishing() {
    let reference = unique_channel_reference("replay-publish-join");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "history", &["h1"]).await;
    settle().await;

    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&receiver, "history");
    let delivered = collect(&history);
    let replay = await_body(&history, "h1");
    publish_all(&receiver, "history", &["joining"]).await;
    replay.wait().await;
    settle().await;

    assert_payloads(&delivered, &["h1"]);
} // end fn replays_to_a_segment_joined_by_publishing

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_what_a_rejoin_missed_and_drops_what_it_already_delivered() {
    let reference = unique_channel_reference("replay-rejoin");
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&receiver, "history");
    let delivered = collect(&history);
    let first = subscribe(&history);
    settle().await;

    let one = await_body(&history, "one");
    publish_all(&publisher, "history", &["one"]).await;
    one.wait().await;

    first.cancel();
    settle().await;
    publish_all(&publisher, "history", &["two"]).await;
    settle().await;

    // The re-join replays "one" and "two"; the deduplication window drops
    // "one".
    let two = await_body(&history, "two");
    subscribe(&history);
    two.wait().await;

    let three = await_body(&history, "three");
    publish_all(&publisher, "history", &["three"]).await;
    three.wait().await;
    settle().await;

    assert_payloads(&delivered, &["one", "two", "three"]);
    assert_eq!(distinct_message_ids(&delivered), 3);
} // end fn recovers_what_a_rejoin_missed_and_drops_what_it_already_delivered

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_to_the_channel_listener_one_time_for_each_message() {
    let reference = unique_channel_reference("replay-channel-listener");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "history", &["h1", "h2"]).await;
    settle().await;

    let receiver = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let (seen, _registration) = collect_channel(&receiver);
    let both = await_channel_body(&receiver, "h2");
    let membership = subscribe(&segment(&receiver, "history"));
    both.wait().await;

    // A re-join replays both again; the channel listener sees neither twice.
    membership.cancel();
    settle().await;
    subscribe(&segment(&receiver, "history"));
    sleep(Duration::from_secs(4)).await;

    assert_eq!(tagged(&seen), ["history:h1", "history:h2"]);
} // end fn replays_to_the_channel_listener_one_time_for_each_message
