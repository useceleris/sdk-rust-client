//! Rate limits and the command queue (catalogue section 10, RESEND-01).

use std::time::Duration;

use tokio::time::Instant;

use crate::channel::Channel;
use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::tests::support::{
    Peer, client_with_server, connect, connected_channel, publish_command, publish_now,
    rate_limit_frame, receive_rate_limit, record_errors, record_states, segment_command, settle,
    start_publish, test_options, yield_until,
};
use crate::values::ChannelState;

fn sub(segment_id: &str) -> String {
    segment_command("SUB", segment_id)
} // end fn sub

/// Reports eight limits in a row, each after the previous resend round, so the
/// next limit is treated as a used-up quota. The channel holds a subscription
/// to "chat" and nothing else.
async fn exhaust_rate_limit(channel: &Channel, peer: &mut Peer) {
    for _ in 0..8 {
        receive_rate_limit(channel, peer).await;
        assert_eq!(peer.next_command().await, sub("chat"));
    }
} // end fn exhaust_rate_limit

#[tokio::test(start_paused = true)]
async fn rate_limit_pauses_then_resends_subscriptions_before_publishes() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = record_errors(&channel).await;
    let chat = channel.segment("chat").unwrap();
    let lobby = channel.segment("lobby").unwrap();
    let _messages = chat.subscribe().unwrap();
    let _presence = chat.subscribe_presence().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "chat")
    );
    publish_now(&lobby, &mut peer, "m-1", "a").await;

    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    let queued = start_publish(&lobby, "m-2", "b").await;

    // Nothing at 999 ms; everything at exactly 1 000 ms with zero jitter.
    assert_eq!(
        peer.commands_within(Duration::from_millis(999)).await,
        Vec::<String>::new()
    );
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "chat")
    );
    assert_eq!(
        peer.next_command().await,
        publish_command("lobby", "m-1", "a")
    );
    assert_eq!(
        peer.next_command().await,
        publish_command("lobby", "m-2", "b")
    );
    queued.await.unwrap();
    settle(&channel).await;

    let limit = ServerError {
        error_type: ServerErrorType::RateLimitError,
        sub_type: None,
        message: "Rate limit exceeded".to_owned(),
        resource: ServerErrorResource::Null,
    };

    assert_eq!(errors.all(), [CelerisError::Server(limit)]);
} // end fn rate_limit_pauses_then_resends_subscriptions_before_publishes

#[tokio::test(start_paused = true)]
async fn rate_limit_resends_a_command_sent_exactly_2000ms_before_not_one_sent_2001ms_before() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    publish_now(&chat, &mut peer, "old", "old").await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    publish_now(&chat, &mut peer, "edge", "edge").await;
    tokio::time::sleep(Duration::from_millis(2_000)).await;

    receive_rate_limit(&channel, &mut peer).await;

    // "old" would go first if it were resent.
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "edge", "edge")
    );

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("control"));
} // end fn rate_limit_resends_a_command_sent_exactly_2000ms_before_not_one_sent_2001ms_before

#[tokio::test(start_paused = true)]
async fn rate_limit_resends_a_publish_byte_for_byte_with_its_original_id_only_once() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let (published, original) = tokio::join!(chat.publish("x"), peer.next_frame());
    published.unwrap();

    receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_frame().await, original);

    // A second limit does not resend it again.
    receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(
        peer.commands_within(Duration::from_secs(5)).await,
        Vec::<String>::new()
    );

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("control"));
} // end fn rate_limit_resends_a_publish_byte_for_byte_with_its_original_id_only_once

#[tokio::test(start_paused = true)]
async fn subscription_toggled_while_paused_sends_one_command() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    receive_rate_limit(&channel, &mut peer).await;

    chat.subscribe().unwrap().cancel();
    let _subscription = chat.subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("control"));
} // end fn subscription_toggled_while_paused_sends_one_command

#[tokio::test(start_paused = true)]
async fn rate_limit_backs_off_then_starts_over_after_a_quiet_window() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 1.0);
    let (channel, mut peer) = connect(&client, &server).await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));

    // 1 s plus the reconnect delay for the streak index, capped at 30 s: the
    // seventh limit reaches the cap and the eighth stays at it.
    let pauses = [1_500, 2_000, 3_000, 5_000, 9_000, 17_000, 31_000, 31_000];

    for pause in pauses {
        let limited_at = receive_rate_limit(&channel, &mut peer).await;
        assert_eq!(peer.next_command().await, sub("chat"));
        assert_eq!(Instant::now() - limited_at, Duration::from_millis(pause));
    }

    // Past the last pause and its suspect window, the streak starts over.
    assert_eq!(
        peer.commands_within(Duration::from_secs(40)).await,
        Vec::<String>::new()
    );
    let _lobby = channel.segment("lobby").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("lobby"));

    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("lobby"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_500));
} // end fn rate_limit_backs_off_then_starts_over_after_a_quiet_window

#[tokio::test(start_paused = true)]
async fn several_frames_for_one_burst_count_once() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));

    // Eight episodes, each reported through two frames: still eight resend
    // rounds, not a give-up after four.
    for _ in 0..8 {
        peer.send(rate_limit_frame()).await;
        receive_rate_limit(&channel, &mut peer).await;
        assert_eq!(peer.next_command().await, sub("chat"));
    }

    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(60_000));
} // end fn several_frames_for_one_burst_count_once

#[tokio::test(start_paused = true)]
async fn backlog_written_during_the_pause_is_not_a_new_episode() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    for burst in 0..8 {
        let message_id = format!("m-{burst}");
        let frame = publish_command("chat", &message_id, "x");

        // Handed to the writer before the limit, written during the pause.
        peer.hold_writes();
        let publish = start_publish(&chat, &message_id, "x").await;
        yield_until("the held write", || peer.has_held_write()).await;
        let limited_at = receive_rate_limit(&channel, &mut peer).await;
        peer.release_writes();
        assert_eq!(peer.next_command().await, frame);
        publish.await.unwrap();

        // A second frame for the same burst.
        peer.send(rate_limit_frame()).await;
        assert_eq!(peer.next_command().await, frame);
        assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    }
} // end fn backlog_written_during_the_pause_is_not_a_new_episode

#[tokio::test(start_paused = true)]
async fn drops_recent_publishes_and_keeps_subscriptions_for_the_probe_after_eight_limits_in_a_row()
{
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.segment("lobby").unwrap();
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));

    for _ in 0..7 {
        receive_rate_limit(&channel, &mut peer).await;
        assert_eq!(peer.next_command().await, sub("chat"));
    }

    // The eighth still resends.
    publish_now(&lobby, &mut peer, "m-8", "x").await;
    receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(
        peer.next_command().await,
        publish_command("lobby", "m-8", "x")
    );

    // The ninth does not: the publish is dropped, and the subscription waits
    // for the probe.
    publish_now(&lobby, &mut peer, "m-9", "x").await;
    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(60_000));

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("control"));
} // end fn drops_recent_publishes_and_keeps_subscriptions_for_the_probe_after_eight_limits_in_a_row

#[tokio::test(start_paused = true)]
async fn quota_probe_resends_dropped_subscriptions_on_a_doubling_schedule_up_to_one_hour() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));
    exhaust_rate_limit(&channel, &mut peer).await;

    let minute = 60_000;
    let delays = [1, 2, 4, 8, 16, 32, 60, 60].map(|minutes| minutes * minute);

    for delay in delays {
        let limited_at = receive_rate_limit(&channel, &mut peer).await;
        assert_eq!(peer.next_command().await, sub("chat"));
        assert_eq!(Instant::now() - limited_at, Duration::from_millis(delay));
    }
} // end fn quota_probe_resends_dropped_subscriptions_on_a_doubling_schedule_up_to_one_hour

#[tokio::test(start_paused = true)]
async fn resending_returns_once_commands_go_a_window_without_a_limit() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));
    exhaust_rate_limit(&channel, &mut peer).await;
    receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("chat"));

    // The probe went out 3 s ago with no limit after it: the quota is back.
    assert_eq!(
        peer.commands_within(Duration::from_secs(3)).await,
        Vec::<String>::new()
    );
    let _lobby = channel.segment("lobby").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("lobby"));

    let limited_at = receive_rate_limit(&channel, &mut peer).await;
    assert_eq!(peer.next_command().await, sub("lobby"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
} // end fn resending_returns_once_commands_go_a_window_without_a_limit

#[tokio::test(start_paused = true)]
async fn quota_probing_survives_a_reconnect() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));
    exhaust_rate_limit(&channel, &mut peer).await;
    receive_rate_limit(&channel, &mut peer).await;

    drop(peer);
    let mut restored = server.accept().await;
    assert_eq!(restored.next_command().await, sub("chat"));

    // The probe schedule continues where it was: the second probe doubles.
    let limited_at = receive_rate_limit(&channel, &mut restored).await;
    assert_eq!(restored.next_command().await, sub("chat"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(120_000));
} // end fn quota_probing_survives_a_reconnect

#[tokio::test(start_paused = true)]
async fn subscription_change_never_overtakes_an_earlier_publish_to_its_segment() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    receive_rate_limit(&channel, &mut peer).await;

    let subscription = chat.subscribe().unwrap();
    let published = start_publish(&chat, "m-1", "x").await;
    subscription.cancel();

    // Publishing joins the segment, so the UNSUB has to follow it.
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-1", "x")
    );
    assert_eq!(peer.next_command().await, segment_command("UNSUB", "chat"));
    published.await.unwrap();
} // end fn subscription_change_never_overtakes_an_earlier_publish_to_its_segment

#[tokio::test(start_paused = true)]
async fn subscription_change_waits_only_for_publishes_queued_before_it() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    receive_rate_limit(&channel, &mut peer).await;

    let first = start_publish(&chat, "m-1", "1").await;
    let _subscription = chat.subscribe().unwrap();
    let second = start_publish(&chat, "m-2", "2").await;

    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-1", "1")
    );
    assert_eq!(peer.next_command().await, sub("chat"));
    assert_eq!(
        peer.next_command().await,
        publish_command("chat", "m-2", "2")
    );
    first.await.unwrap();
    second.await.unwrap();
} // end fn subscription_change_waits_only_for_publishes_queued_before_it

#[tokio::test(start_paused = true)]
async fn rate_limit_resends_at_most_the_last_64_publishes() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.segment("lobby").unwrap();

    for index in 1..=65 {
        publish_now(&lobby, &mut peer, &format!("m-{index}"), &index.to_string()).await;
    }

    receive_rate_limit(&channel, &mut peer).await;

    for index in 2..=65 {
        assert_eq!(
            peer.next_command().await,
            publish_command("lobby", &format!("m-{index}"), &index.to_string())
        );
    }

    let _control = channel.segment("control").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("control"));
} // end fn rate_limit_resends_at_most_the_last_64_publishes

#[tokio::test(start_paused = true)]
async fn presence_query_while_paused_is_backpressure() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    receive_rate_limit(&channel, &mut peer).await;

    let result = channel.segment("chat").unwrap().presence_list(1, 25).await;

    assert_eq!(
        result,
        Err(CelerisError::Backpressure(
            "Sending is paused after a rate limit; try again in a moment.".to_owned()
        ))
    );
} // end fn presence_query_while_paused_is_backpressure

#[tokio::test(start_paused = true)]
async fn restored_subscriptions_are_resent_after_a_rate_limit() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, sub("chat"));
    drop(peer);

    let mut restored = server.accept().await;
    assert_eq!(restored.next_command().await, sub("chat"));

    let limited_at = receive_rate_limit(&channel, &mut restored).await;
    assert_eq!(restored.next_command().await, sub("chat"));
    assert_eq!(Instant::now() - limited_at, Duration::from_millis(1_000));
    assert_eq!(channel.state(), ChannelState::Connected);
} // end fn restored_subscriptions_are_resent_after_a_rate_limit

#[tokio::test(start_paused = true)]
async fn refused_subscription_write_replaces_the_socket() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let errors = record_errors(&channel).await;
    let states = record_states(&channel).await;
    peer.fail_writes();

    let _chat = channel.segment("chat").unwrap().subscribe().unwrap();
    let mut restored = server.accept().await;
    assert_eq!(restored.next_command().await, sub("chat"));
    settle(&channel).await;

    assert_eq!(
        states.all(),
        [ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(errors.all(), Vec::<CelerisError>::new());
    assert_eq!(channel.state(), ChannelState::Connected);

    // The client let go of the refused socket.
    assert_eq!(peer.next_message().await, None);
} // end fn refused_subscription_write_replaces_the_socket
