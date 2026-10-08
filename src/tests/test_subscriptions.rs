use std::time::Duration;

use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, Peer, Recorder, client_with_server, connected_channel, delivery_frame,
    error_recorder, record_message_ids, segment_command, settle, test_options, wait_until,
};
use crate::values::{ChannelState, MessageMetadata};

/// Publishes a control message and asserts that it is the next frame the peer
/// reads, so nothing was sent before it.
async fn assert_next_frame_is_control_publish(channel: &crate::channel::Channel, peer: &mut Peer) {
    let control = channel.segment("control").unwrap();

    let (published, frame) = tokio::join!(
        control.publish_with_message_id(b"c", "control-1"),
        peer.next_command()
    );

    published.unwrap();
    assert_eq!(frame, "@PUB\n$7\ncontrol\n$9\ncontrol-1\n$1\nc\n");
} // end fn assert_next_frame_is_control_publish

fn metadata(segment_id: &str, message_id: &str, timestamp: i64) -> MessageMetadata {
    MessageMetadata {
        token_reference: "user".to_owned(),
        segment_id: segment_id.to_owned(),
        message_id: message_id.to_owned(),
        timestamp,
    }
} // end fn metadata

#[tokio::test(start_paused = true)]
async fn segment_handles_are_stateless_and_invalid_segment_ids_are_refused() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let first = channel.segment("chat").unwrap();
    let second = channel.segment("chat").unwrap();

    assert_eq!(first.id(), "chat");
    assert_eq!(second.id(), "chat");
    assert_eq!(channel.default_segment().id(), "default");

    let invalid = [
        ("", "Invalid segment ID. Must not be empty."),
        ("bad\nid", "Invalid segment ID. Must not contain CR or LF."),
        ("bad\rid", "Invalid segment ID. Must not contain CR or LF."),
    ];

    for (segment_id, message) in invalid {
        assert_eq!(
            channel.segment(segment_id).unwrap_err(),
            CelerisError::Configuration(message.to_owned())
        );
    }

    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn segment_handles_are_stateless_and_invalid_segment_ids_are_refused

#[tokio::test(start_paused = true)]
async fn segments_share_one_interest_count_and_the_last_cancel_sends_unsub() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let first = channel.segment("chat").unwrap().subscribe().unwrap();
    let second = channel.segment("chat").unwrap().subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    first.cancel();
    first.cancel();
    second.cancel();
    second.cancel();

    // One SUB, then one UNSUB: the first handle's cancels sent nothing.
    assert_eq!(peer.next_command().await, segment_command("UNSUB", "chat"));
    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn segments_share_one_interest_count_and_the_last_cancel_sends_unsub

#[tokio::test(start_paused = true)]
async fn default_segment_never_sends_sub_or_unsub() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let membership = channel.default_segment().subscribe().unwrap();
    membership.cancel();

    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    // Nothing follows the control either.
    let later = peer.frames_within(Duration::from_millis(2_500)).await;
    assert_eq!(later, Vec::<String>::new());

    channel.close().await;
} // end fn default_segment_never_sends_sub_or_unsub

#[tokio::test(start_paused = true)]
async fn listener_alone_sends_nothing() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    channel.segment("chat").unwrap().on_message(|_, _| {});
    channel.events().on_message(|_, _| {});

    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    // Nothing follows the control either.
    let later = peer.frames_within(Duration::from_millis(2_500)).await;
    assert_eq!(later, Vec::<String>::new());

    channel.close().await;
} // end fn listener_alone_sends_nothing

#[tokio::test(start_paused = true)]
async fn held_interests_are_sent_on_connect_in_registration_order_messages_first() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();

    let _beta = channel.segment("beta").unwrap().subscribe().unwrap();
    let _alpha = channel.segment("alpha").unwrap().subscribe().unwrap();
    let _default = channel.default_segment().subscribe().unwrap();
    let _gamma = channel
        .segment("gamma")
        .unwrap()
        .subscribe_presence()
        .unwrap();
    let _default_presence = channel.default_segment().subscribe_presence().unwrap();
    let gone = channel.segment("gone").unwrap().subscribe().unwrap();
    let gone_presence = channel
        .segment("gone")
        .unwrap()
        .subscribe_presence()
        .unwrap();
    gone.cancel();
    gone_presence.cancel();

    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    assert_eq!(peer.next_command().await, segment_command("SUB", "beta"));
    assert_eq!(peer.next_command().await, segment_command("SUB", "alpha"));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "gamma")
    );
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "default")
    );
    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn held_interests_are_sent_on_connect_in_registration_order_messages_first

#[tokio::test(start_paused = true)]
async fn more_than_64_held_interests_are_sent_as_the_writer_drains() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let mut subscriptions = Vec::new();

    for index in 0..65 {
        let segment = channel.segment(&format!("segment-{index}")).unwrap();
        subscriptions.push(segment.subscribe().unwrap());
    }

    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    // The writer takes 64; the last goes out once a write finishes.
    for index in 0..65 {
        assert_eq!(
            peer.next_command().await,
            segment_command("SUB", &format!("segment-{index}"))
        );
    }

    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn more_than_64_held_interests_are_sent_as_the_writer_drains

#[tokio::test(start_paused = true)]
async fn failed_connect_leaves_nothing_for_the_next_connect() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let subscription = channel.segment("chat").unwrap().subscribe().unwrap();

    server.set_behaviour(DialBehaviour::Refuse);

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Transport(
            "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference."
                .to_owned()
        ))
    );

    assert_eq!(channel.state(), ChannelState::Failed);

    subscription.cancel();
    server.set_behaviour(DialBehaviour::Accept);
    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    let _lobby = channel.segment("lobby").unwrap().subscribe().unwrap();

    assert_eq!(peer.next_command().await, segment_command("SUB", "lobby"));
    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn failed_connect_leaves_nothing_for_the_next_connect

#[tokio::test(start_paused = true)]
async fn message_cancel_sends_unsub_also_while_a_presence_interest_is_held() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let messages = chat.subscribe().unwrap();
    let presence = chat.subscribe_presence().unwrap();

    messages.cancel();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "chat")
    );
    assert_eq!(peer.next_command().await, segment_command("UNSUB", "chat"));

    presence.cancel();
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_UNSUB", "chat")
    );

    channel.close().await;
} // end fn message_cancel_sends_unsub_also_while_a_presence_interest_is_held

#[tokio::test(start_paused = true)]
async fn messages_go_only_to_their_segment_and_every_handle_gets_them() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = record_message_ids(&channel.segment("chat").unwrap());
    let lobby = record_message_ids(&channel.default_segment());
    let every = Recorder::new();

    for _ in 0..2 {
        let collector = every.clone();

        channel
            .segment("all")
            .unwrap()
            .on_message(move |payload, metadata| {
                collector.push((payload.to_vec(), metadata.clone()))
            });
    }

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"hi"))
        .await;
    peer.send(delivery_frame("user", "default", "id-2", 2, b"yo"))
        .await;
    peer.send(delivery_frame("user", "other", "id-3", 3, b"no"))
        .await;
    peer.send(delivery_frame("user", "all", "id-4", 4, b"both"))
        .await;
    wait_until("the last delivery", || every.len() == 2).await;
    settle(&channel).await;

    assert_eq!(chat.all(), vec!["id-1".to_owned()]);
    assert_eq!(lobby.all(), vec!["id-2".to_owned()]);

    let delivered = (b"both".to_vec(), metadata("all", "id-4", 4));
    assert_eq!(every.all(), vec![delivered.clone(), delivered]);

    channel.close().await;
} // end fn messages_go_only_to_their_segment_and_every_handle_gets_them

#[tokio::test(start_paused = true)]
async fn channel_listener_gets_all_segments_after_segment_listeners_each_id_once() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let order = Recorder::new();
    let seen = Recorder::new();

    let collector = order.clone();
    channel
        .events()
        .on_message(move |_, _| collector.push("channel"));

    let collector = order.clone();
    channel
        .segment("chat")
        .unwrap()
        .on_message(move |_, _| collector.push("segment"));

    let collector = seen.clone();

    channel
        .events()
        .on_message(move |payload, metadata| collector.push((payload.to_vec(), metadata.clone())));

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"hi"))
        .await;
    peer.send(delivery_frame("user", "default", "id-2", 2, b"yo"))
        .await;
    peer.send(delivery_frame(
        "user",
        "joined-by-publish",
        "id-3",
        3,
        b"ok",
    ))
    .await;

    // A duplicate in another segment, and a delivery without an id.
    peer.send(delivery_frame("user", "other", "id-1", 4, b"hi"))
        .await;
    peer.send(b"@MSG\n+user\n+chat\n$-1\n:5\n$1\nx\n".to_vec())
        .await;
    peer.send(delivery_frame("user", "chat", "id-4", 6, b"last"))
        .await;
    wait_until("the last delivery", || seen.len() == 4).await;
    settle(&channel).await;

    assert_eq!(
        seen.all(),
        vec![
            (b"hi".to_vec(), metadata("chat", "id-1", 1)),
            (b"yo".to_vec(), metadata("default", "id-2", 2)),
            (b"ok".to_vec(), metadata("joined-by-publish", "id-3", 3)),
            (b"last".to_vec(), metadata("chat", "id-4", 6)),
        ]
    );

    // Segment listeners first, for each of the two chat deliveries.
    assert_eq!(
        order.all(),
        vec![
            "segment", "channel", "channel", "channel", "segment", "channel"
        ]
    );

    assert_eq!(
        errors.all(),
        vec![CelerisError::Protocol {
            message: "Server message is missing its identifier.".to_owned(),
            field: "MessageID",
            offset: 0,
        }]
    );

    channel.close().await;
} // end fn channel_listener_gets_all_segments_after_segment_listeners_each_id_once

#[tokio::test(start_paused = true)]
async fn channel_listener_remove_removes_only_that_listener_and_sends_nothing() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let delivered = Recorder::new();

    let collector = delivered.clone();
    let removed = channel
        .events()
        .on_message(move |_, _| collector.push("removed"));

    let collector = delivered.clone();
    channel
        .events()
        .on_message(move |_, _| collector.push("channel"));

    let collector = delivered.clone();
    let chat = channel.segment("chat").unwrap();
    chat.on_message(move |_, _| collector.push("segment"));
    let _subscription = chat.subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    removed.remove();
    removed.remove();

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 2).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["segment", "channel"]);
    assert_next_frame_is_control_publish(&channel, &mut peer).await;

    channel.close().await;
} // end fn channel_listener_remove_removes_only_that_listener_and_sends_nothing

#[tokio::test(start_paused = true)]
async fn sdk_delivers_whatever_the_subscription_state() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let delivered = record_message_ids(&chat);
    chat.subscribe().unwrap().cancel();

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);

    channel.close().await;
} // end fn sdk_delivers_whatever_the_subscription_state
