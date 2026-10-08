use crate::client::ClientOptions;
use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, Recorder, client_with_server, connected_channel, delivery_frame, error_recorder,
    notice_frame, record_message_ids, segment_command, settle, test_options, wait_until,
};
use crate::values::ChannelState;

#[tokio::test(start_paused = true)]
async fn ids_are_recorded_before_fan_out_also_with_no_listener() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let notices = Recorder::new();
    let collector = notices.clone();
    channel
        .events()
        .on_notice(move |notice| collector.push(notice.timestamp));

    // No message listener exists yet. The notice after it shows that id-1 was
    // routed.
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    peer.send(notice_frame(1, "routed")).await;
    wait_until("the notice", || notices.len() == 1).await;

    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    for message_id in ["id-1", "id-2", "id-2", "id-3"] {
        peer.send(delivery_frame("user", "chat", message_id, 2, b"x"))
            .await;
    }

    wait_until("id-3", || delivered.len() == 2).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-2".to_owned(), "id-3".to_owned()]);

    channel.close().await;
} // end fn ids_are_recorded_before_fan_out_also_with_no_listener

#[tokio::test(start_paused = true)]
async fn deduplication_window_survives_a_reconnect_and_clears_on_an_explicit_connect() {
    let (options, _log) = test_options();

    let options = ClientOptions {
        maximum_reconnect_attempts: Some(1),
        ..options
    };

    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let chat = channel.segment("chat").unwrap();
    let delivered = record_message_ids(&chat);
    let _subscription = chat.subscribe().unwrap();

    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("id-1", || delivered.len() == 1).await;

    // A reconnect keeps the window: the replayed id-1 is dropped.
    peer.close().await;
    let mut restored = server.accept().await;
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "chat")
    );

    restored
        .send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    restored
        .send(delivery_frame("user", "chat", "id-2", 2, b"x"))
        .await;
    wait_until("id-2", || delivered.len() == 2).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-1".to_owned(), "id-2".to_owned()]);

    // Recovery fails, and an explicit connect clears the window.
    server.set_behaviour(DialBehaviour::Refuse);
    restored.close().await;
    wait_until("the failed state", || {
        channel.state() == ChannelState::Failed
    })
    .await;

    server.set_behaviour(DialBehaviour::Accept);
    let (connected, mut fresh) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    assert_eq!(fresh.next_command().await, segment_command("SUB", "chat"));

    fresh
        .send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("id-1 again", || delivered.len() == 3).await;
    settle(&channel).await;

    assert_eq!(
        delivered.all(),
        vec!["id-1".to_owned(), "id-2".to_owned(), "id-1".to_owned()]
    );

    channel.close().await;
} // end fn deduplication_window_survives_a_reconnect_and_clears_on_an_explicit_connect

#[tokio::test(start_paused = true)]
async fn message_without_an_id_is_dropped_and_reported_and_the_connection_stays() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    peer.send(b"@MSG\n+user\n+chat\n$-1\n:1\n$1\nx\n".to_vec())
        .await;
    peer.send(delivery_frame("user", "chat", "id-1", 2, b"x"))
        .await;
    wait_until("id-1", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);

    let reported = errors.all();

    assert_eq!(
        reported,
        vec![CelerisError::Protocol {
            message: "Server message is missing its identifier.".to_owned(),
            field: "MessageID",
            offset: 0,
        }]
    );

    assert_eq!(
        reported[0].to_string(),
        "Server message is missing its identifier. Field: MessageID, byte offset 0."
    );
    assert_eq!(channel.state(), ChannelState::Connected);
    assert!(!server.has_pending_connection());

    channel.close().await;
} // end fn message_without_an_id_is_dropped_and_reported_and_the_connection_stays

#[tokio::test(start_paused = true)]
async fn replayed_duplicates_across_a_reconnect_are_dropped_while_new_ids_continue() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let delivered = record_message_ids(&chat);
    let _subscription = chat.subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    peer.send(delivery_frame("user", "chat", "id-1", 1, b"a"))
        .await;
    peer.send(delivery_frame("user", "chat", "id-2", 2, b"b"))
        .await;
    wait_until("id-2", || delivered.len() == 2).await;

    peer.close().await;
    let mut restored = server.accept().await;
    assert_eq!(
        restored.next_command().await,
        segment_command("SUB", "chat")
    );

    // The replay overlap delivers the old ids again.
    for (message_id, timestamp) in [("id-1", 1), ("id-2", 2), ("id-3", 3)] {
        restored
            .send(delivery_frame("user", "chat", message_id, timestamp, b"c"))
            .await;
    }

    wait_until("id-3", || delivered.len() == 3).await;
    settle(&channel).await;

    assert_eq!(
        delivered.all(),
        vec!["id-1".to_owned(), "id-2".to_owned(), "id-3".to_owned()]
    );

    channel.close().await;
} // end fn replayed_duplicates_across_a_reconnect_are_dropped_while_new_ids_continue
