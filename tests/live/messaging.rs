//! Publishing, limits and rate limits (catalogue sections 4, 10 and 12).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time::{Instant, sleep};
use useceleris_client::{CelerisError, ChannelState, ServerErrorResource, ServerErrorType};

use crate::support::channels::{
    connected, pair, publish_and_await, publish_paced, publish_text, segment, subscribe,
    unique_channel_reference,
};
use crate::support::collect::{
    Recorder, assert_payloads, collect, distinct_message_ids, is_generated_message_id, numbered,
    patterned,
};
use crate::support::signer::{Claims, now_millis};
use crate::support::wait::{
    EVENT_WAIT, await_body, await_error, await_message, is_server_error, server_error, settle,
};

/// Sets a flag when the channel reports a RateLimitError.
fn watch_rate_limit(channel: &useceleris_client::Channel) -> Arc<AtomicBool> {
    let limited = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&limited);

    channel.events().on_error(move |error| {
        if is_server_error(error, &ServerErrorType::RateLimitError) {
            flag.store(true, Ordering::SeqCst);
        }
    });

    limited
} // end fn watch_rate_limit

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_binary_payloads_with_their_ids_and_per_connection_echo() {
    let (publisher, receiver) = pair("msg", Claims::default()).await;
    let publisher_chat = segment(&publisher, "chat");
    let publisher_saw = collect(&publisher_chat);
    subscribe(&publisher_chat);
    let chat = segment(&receiver, "chat");
    subscribe(&chat);
    settle().await;

    let body = "hello-바이너리\r\n\u{0}\u{ff}";
    let arrived = await_message(&chat, |_| true, "cross-connection delivery", EVENT_WAIT);

    publisher_chat
        .publish(body.as_bytes())
        .await
        .expect("the publish is accepted");

    let message = arrived.wait().await;

    // REV-01: the id that the SDK generated arrives unchanged.
    assert_eq!(message.payload, body.as_bytes());
    assert!(
        is_generated_message_id(&message.metadata.message_id),
        "message id {:?}",
        message.metadata.message_id
    );

    assert_eq!(message.metadata.segment_id, "chat");
    assert!(message.metadata.timestamp > 0);

    // The publishing connection itself is echo-suppressed.
    sleep(Duration::from_millis(1500)).await;
    assert_payloads(&publisher_saw, &[]);
} // end fn delivers_binary_payloads_with_their_ids_and_per_connection_echo

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn echoes_to_the_publisher_when_the_token_allows_echo() {
    let channel = connected(&unique_channel_reference("echo"), Claims::echo()).await;
    let chat = segment(&channel, "chat");
    subscribe(&chat);
    settle().await;

    let echoed = await_body(&chat, "self");
    publish_text(&channel, "chat", "self").await;
    echoed.wait().await;
} // end fn echoes_to_the_publisher_when_the_token_allows_echo

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_on_the_default_segment_without_subscribing() {
    let (publisher, receiver) = pair("default", Claims::default()).await;
    settle().await;

    let arrived = await_body(&receiver.default_segment(), "lobby");
    publish_text(&publisher, "default", "lobby").await;
    let message = arrived.wait().await;

    assert_eq!(message.metadata.segment_id, "default");
} // end fn delivers_on_the_default_segment_without_subscribing

/// Needs an app on a plan with a payload cap of at least 1024 KiB. Framed,
/// the full payload is larger than 1 MiB (LIMIT-01).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_trips_large_binary_payloads() {
    for size in [100 * 1024, 1024 * 1024] {
        let (publisher, receiver) = pair("large", Claims::default()).await;
        let bulk = segment(&receiver, "bulk");
        subscribe(&bulk);
        settle().await;
        let payload = patterned(size);

        let arrived = await_message(
            &bulk,
            move |message| message.payload.len() == size,
            "the large delivery",
            Duration::from_secs(30),
        );

        segment(&publisher, "bulk")
            .publish(&payload)
            .await
            .expect("the publish is accepted");

        let message = arrived.wait().await;

        assert!(
            message.payload == payload,
            "the {size}-byte payload changed in transit"
        );
    }
} // end fn round_trips_large_binary_payloads

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reports_a_publish_over_the_plan_cap_as_message_size_limit() {
    let channel = connected(&unique_channel_reference("oversize"), Claims::default()).await;

    let rejected = await_error(
        &channel,
        |error| is_server_error(error, &ServerErrorType::MessageSizeLimitError),
        "the MessageSizeLimitError frame",
        Duration::from_secs(20),
    );

    // Over every plan's cap but under the 2 MiB ceiling, so it completes
    // locally before the server refuses it.
    segment(&channel, "bulk")
        .publish(vec![0_u8; 1536 * 1024])
        .await
        .expect("the publish is accepted locally");

    let rejection = rejected.wait().await;

    assert!(
        rejection
            .to_string()
            .contains("Message size limit exceeded"),
        "got {rejection}"
    );

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn reports_a_publish_over_the_plan_cap_as_message_size_limit

/// Edge: 1024 KiB is delivered (above), and one byte more is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_payload_one_byte_over_the_1024_kib_plan_cap() {
    let channel = connected(&unique_channel_reference("cap-plus-one"), Claims::default()).await;

    let rejected = await_error(
        &channel,
        |error| is_server_error(error, &ServerErrorType::MessageSizeLimitError),
        "the MessageSizeLimitError frame",
        Duration::from_secs(20),
    );

    segment(&channel, "bulk")
        .publish(vec![0_u8; 1024 * 1024 + 1])
        .await
        .expect("the publish is accepted locally");

    let rejection = rejected.wait().await;

    assert!(
        rejection.to_string().contains("size limit = 1024 KB"),
        "got {rejection}"
    );

    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn refuses_a_payload_one_byte_over_the_1024_kib_plan_cap

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_command_over_two_mebibytes_locally_and_stays_connected() {
    let (publisher, receiver) = pair("ceiling", Claims::default()).await;
    let bulk = segment(&receiver, "bulk");
    let received = collect(&bulk);
    subscribe(&bulk);
    settle().await;

    let refused = segment(&publisher, "bulk")
        .publish(vec![0_u8; 2 * 1024 * 1024])
        .await;

    assert_eq!(
        refused,
        Err(CelerisError::Configuration(
            "Encoded command exceeds 2 MiB. That is the most the server accepts on any plan; send a smaller payload."
                .to_owned()
        ))
    );

    publish_and_await(&publisher, &receiver, "bulk", "after").await;

    assert_payloads(&received, &["after"]);
    assert_eq!(publisher.state(), ChannelState::Connected);
} // end fn refuses_a_command_over_two_mebibytes_locally_and_stays_connected

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reports_a_read_only_tokens_publish_as_permission_denied() {
    let read_only = connected(
        &unique_channel_reference("perm"),
        Claims::permission(true, false),
    )
    .await;

    let denied = await_error(
        &read_only,
        |error| is_server_error(error, &ServerErrorType::PermissionDeniedError),
        "the PermissionDeniedError frame",
        EVENT_WAIT,
    );

    // The publish completes locally; the denial arrives later through
    // on_error.
    publish_text(&read_only, "chat", "denied").await;
    let denial = denied.wait().await;
    let denial = server_error(&denial).expect("a server error");

    assert_eq!(denial.sub_type.as_deref(), Some("PUB"));
    assert_eq!(
        denial.resource,
        ServerErrorResource::Text("chat".to_owned())
    );
    assert!(!denial.message.is_empty());
    assert_eq!(read_only.state(), ChannelState::Connected);
} // end fn reports_a_read_only_tokens_publish_as_permission_denied

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_a_write_only_token_publishing_while_receiving_nothing() {
    let reference = unique_channel_reference("writeonly");
    let write_only = connected(&reference, Claims::permission(false, true)).await;
    let reader = connected(&reference, Claims::default()).await;
    let writer_chat = segment(&write_only, "chat");
    let writer_saw = collect(&writer_chat);
    subscribe(&writer_chat);
    subscribe(&segment(&reader, "chat"));
    settle().await;

    publish_and_await(&write_only, &reader, "chat", "one-way").await;
    sleep(Duration::from_millis(1500)).await;

    assert_payloads(&writer_saw, &[]);
} // end fn keeps_a_write_only_token_publishing_while_receiving_nothing

/// RES-05: subscriptions that the server drops under its rate limit recover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_subscriptions_the_server_drops_under_its_rate_limit() {
    let reference = unique_channel_reference("limit");

    // Separate token references keep separate per-connection limits.
    let subscriber = connected(&reference, Claims::reference("limit-subscriber")).await;
    let publisher = connected(&reference, Claims::reference("limit-publisher")).await;
    let limited = watch_rate_limit(&subscriber);
    let delivered: Recorder<String> = Recorder::default();
    let mut segment_ids: Vec<String> = Vec::new();

    // The limiter tolerates bursts, so subscriptions go out in growing
    // batches until one trips it. Some are then dropped, and only recovery
    // can restore them.
    while !limited.load(Ordering::SeqCst) && segment_ids.len() < 2000 {
        for _ in 0..250 {
            let segment_id = format!("limit-{}", segment_ids.len());
            let handle = segment(&subscriber, &segment_id);
            let sink = delivered.clone();
            let id = segment_id.clone();
            handle.on_message(move |_, _| sink.push(id.clone()));
            subscribe(&handle);
            segment_ids.push(segment_id);
        }

        sleep(Duration::from_millis(500)).await;
    }

    assert!(
        limited.load(Ordering::SeqCst),
        "the subscription burst must trip the limit"
    );

    // Publish to every segment not yet delivered, round after round, until
    // each subscription has recovered. A publish that the publisher's own
    // limit drops is published again next round.
    let deadline = Instant::now() + Duration::from_secs(150);

    let pending = || {
        let seen: HashSet<String> = delivered.all().into_iter().collect();

        segment_ids
            .iter()
            .filter(|segment_id| !seen.contains(*segment_id))
            .cloned()
            .collect::<Vec<String>>()
    };

    while !pending().is_empty() && Instant::now() < deadline {
        sleep(Duration::from_secs(3)).await;

        for segment_id in pending() {
            let result = segment(&publisher, &segment_id)
                .publish(segment_id.as_bytes())
                .await;

            match result {
                Ok(()) => {}

                // The publisher trips its own limit: let it drain.
                Err(CelerisError::Backpressure(_)) => sleep(Duration::from_secs(2)).await,

                // Under this load the server sheds connections. The channel
                // recovers by itself, and restores its subscriptions.
                Err(CelerisError::NotConnected(_)) => sleep(Duration::from_secs(1)).await,
                Err(error) => panic!("publish to {segment_id}: {error}"),
            }

            sleep(Duration::from_millis(10)).await;
        }
    }

    let missing = pending();

    assert!(
        missing.is_empty(),
        "{} of {} subscriptions never recovered",
        missing.len(),
        segment_ids.len()
    );
} // end fn recovers_subscriptions_the_server_drops_under_its_rate_limit

/// RESEND-01: publishes that a rate limit may have dropped are resent with
/// their first ids, so the receiver drops every copy that got through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_no_duplicate_under_a_publish_rate_limit() {
    let reference = unique_channel_reference("limit-publish");

    // Separate token references keep separate per-connection limits.
    let receiver = connected(&reference, Claims::reference("limit-receiver")).await;
    let publisher = connected(&reference, Claims::reference("limit-publisher")).await;
    let receiver_burst = segment(&receiver, "burst");
    let received = collect(&receiver_burst);
    subscribe(&receiver_burst);
    settle().await;

    let limited = watch_rate_limit(&publisher);
    let burst = segment(&publisher, "burst");
    let mut published: HashSet<String> = HashSet::new();
    let mut size = 100;

    // The limiter tolerates bursts, so publishes go out in growing bursts
    // until one trips it, with a ceiling on the total so that the test ends.
    while !limited.load(Ordering::SeqCst) && published.len() < 5000 {
        for _ in 0..size {
            let body = format!("b-{}", published.len());
            published.insert(body.clone());

            match burst.publish(body.as_bytes()).await {
                Ok(()) => {}

                // Sending is paused after a limit: this burst is over.
                Err(CelerisError::Backpressure(_)) => break,
                Err(error) => panic!("publish of {body}: {error}"),
            }
        }

        size *= 2;
        sleep(Duration::from_millis(500)).await;
    }

    assert!(
        limited.load(Ordering::SeqCst),
        "the publish bursts must trip the limit"
    );

    sleep(Duration::from_secs(3)).await;

    let marker = await_message(
        &receiver_burst,
        |message| message.payload == b"marker",
        "the marker",
        Duration::from_secs(30),
    );

    publish_text(&publisher, "burst", "marker").await;
    marker.wait().await;

    let mut seen = HashSet::new();

    for message in received.all() {
        let body = message.text();

        assert!(
            seen.insert(message.metadata.message_id.clone()),
            "message id {} delivered twice",
            message.metadata.message_id
        );

        assert!(
            published.contains(&body) || body == "marker",
            "delivered {body:?}, which was never published"
        );
    }

    assert_eq!(publisher.state(), ChannelState::Connected);
    assert_eq!(receiver.state(), ChannelState::Connected);
} // end fn delivers_no_duplicate_under_a_publish_rate_limit

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_a_custom_message_id_unchanged_and_drops_a_repeat_of_it() {
    let (publisher, receiver) = pair("custom-id", Claims::default()).await;
    let chat = segment(&receiver, "chat");
    let received = collect(&chat);
    subscribe(&chat);
    settle().await;
    let message_id = format!("order-{}", now_millis());

    for body in ["first", "repeat"] {
        segment(&publisher, "chat")
            .publish_with_message_id(body.as_bytes(), &message_id)
            .await
            .expect("the publish is accepted");
    }

    publish_and_await(&publisher, &receiver, "chat", "marker").await;

    assert_payloads(&received, &["first", "marker"]);
    assert_eq!(received.all()[0].metadata.message_id, message_id);
} // end fn delivers_a_custom_message_id_unchanged_and_drops_a_repeat_of_it

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_trips_an_empty_payload() {
    let (publisher, receiver) = pair("empty", Claims::default()).await;
    let chat = segment(&receiver, "chat");
    subscribe(&chat);
    settle().await;

    let arrived = await_message(
        &chat,
        |message| message.payload.is_empty(),
        "the empty payload",
        EVENT_WAIT,
    );

    segment(&publisher, "chat")
        .publish(b"")
        .await
        .expect("the publish is accepted");

    let message = arrived.wait().await;

    assert!(
        is_generated_message_id(&message.metadata.message_id),
        "message id {:?}",
        message.metadata.message_id
    );
} // end fn round_trips_an_empty_payload

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_a_paced_burst_of_50_messages_in_publish_order_one_time_each() {
    let (publisher, receiver) = pair("burst", Claims::default()).await;
    let chat = segment(&receiver, "chat");
    let received = collect(&chat);
    subscribe(&chat);
    settle().await;
    let bodies = numbered("b", 50);
    let bodies: Vec<&str> = bodies.iter().map(String::as_str).collect();

    let last = await_message(
        &chat,
        |message| message.payload == b"b49",
        "the last message of the burst",
        Duration::from_secs(30),
    );

    publish_paced(&publisher, "chat", &bodies).await;
    last.wait().await;
    settle().await;

    assert_payloads(&received, &bodies);
    assert_eq!(distinct_message_ids(&received), 50);
} // end fn delivers_a_paced_burst_of_50_messages_in_publish_order_one_time_each
