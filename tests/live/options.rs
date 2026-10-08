//! Client options at values other than the defaults (catalogue section 1).

use std::time::Duration;

use tokio::time::{sleep, timeout};
use useceleris_client::{CelerisError, ChannelState, ClientOptions};

use crate::support::channels::{
    Opened, connect, connected, open, options, publish_and_await, publish_text, segment,
    signing_provider, subscribe, unique_channel_reference,
};
use crate::support::collect::{assert_payloads, collect};
use crate::support::environment::websocket_url;
use crate::support::proxy::DroppingProxy;
use crate::support::signer::{Claims, Replay};
use crate::support::wait::settle;

/// A connected channel at `base_url` whose client has the options that
/// `configure` sets.
async fn channel_with(
    base_url: &str,
    reference: &str,
    claims: Claims,
    configure: impl FnOnce(ClientOptions) -> ClientOptions,
) -> Opened {
    let channel = open(
        configure(options(base_url, signing_provider(claims))),
        reference,
    );

    connect(&channel).await;

    channel
} // end fn channel_with

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn times_out_a_presence_query_at_a_1ms_presence_timeout_and_stays_connected() {
    let channel = channel_with(
        websocket_url(),
        &unique_channel_reference("option-presence"),
        Claims::default(),
        |options| ClientOptions {
            presence_query_timeout: Some(Duration::from_millis(1)),
            ..options
        },
    )
    .await;

    let result = segment(&channel, "room").presence_list(1, 10).await;

    assert_eq!(
        result,
        Err(CelerisError::Timeout(
            "Presence query timed out after 1 ms.".to_owned()
        ))
    );

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn times_out_a_presence_query_at_a_1ms_presence_timeout_and_stays_connected

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_replayed_ids_again_beyond_a_deduplication_window_of_1() {
    let reference = unique_channel_reference("option-window");
    let publisher = connected(&reference, Claims::default()).await;

    let receiver = channel_with(
        websocket_url(),
        &reference,
        Claims::replay(Replay::Backlog),
        |options| ClientOptions {
            deduplication_window_size: Some(1),
            ..options
        },
    )
    .await;

    let history = segment(&receiver, "history");
    let received = collect(&history);
    let first = subscribe(&history);
    settle().await;

    publish_text(&publisher, "history", "one").await;
    publish_and_await(&publisher, &receiver, "history", "two").await;

    // The window holds only "two". The re-join replays "one", which pushes
    // "two" out of the window, so the replayed "two" is delivered again too.
    first.cancel();
    settle().await;
    subscribe(&history);
    sleep(Duration::from_secs(4)).await;

    assert_payloads(&received, &["one", "two", "one", "two"]);
} // end fn delivers_replayed_ids_again_beyond_a_deduplication_window_of_1

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_the_second_waiting_publish_with_a_publish_queue_of_1() {
    let proxy = DroppingProxy::start().await;

    let channel = channel_with(
        proxy.url(),
        &unique_channel_reference("option-queue"),
        Claims::default(),
        |options| ClientOptions {
            publish_queue_size: Some(1),
            ..options
        },
    )
    .await;

    let bulk = segment(&channel, "bulk");

    // The proxy stops reading, so the socket buffer fills and publishes wait.
    proxy.stall_upstream(true);
    let payload = vec![0_u8; 900 * 1024];
    let mut publishes = Vec::new();

    for _ in 0..40 {
        let bulk = bulk.clone();
        let payload = payload.clone();
        publishes.push(tokio::spawn(async move { bulk.publish(payload).await }));
    }

    sleep(Duration::from_secs(2)).await;
    proxy.stall_upstream(false);
    let mut results = Vec::new();

    for publish in publishes {
        let outcome = timeout(Duration::from_secs(60), publish)
            .await
            .unwrap_or_else(|_| panic!("{} of 40 publishes settled", results.len()))
            .expect("the publish task finishes");

        results.push(outcome);
    }

    let refused = Err(CelerisError::Backpressure(
        "The publish queue is full (size 1). Retry once some publishes have gone out.".to_owned(),
    ));

    assert!(
        results.contains(&refused) && results.contains(&Ok(())),
        "results {results:?}"
    );
} // end fn refuses_the_second_waiting_publish_with_a_publish_queue_of_1
