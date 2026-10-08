use std::time::Duration;

use crate::tests::support::{Recorder, connected_channel, delivery_frame, segment_command, settle};
use crate::values::ChannelState;

#[tokio::test(start_paused = true)]
async fn connects_subscribes_publishes_receives_and_closes() {
    let (channel, mut peer, _server, log) = connected_channel().await;
    assert_eq!(channel.state(), ChannelState::Connected);
    assert_eq!(log.requests.len(), 1);

    let chat = channel.segment("chat").unwrap();
    let received = Recorder::new();
    let collector = received.clone();
    chat.on_message(move |payload, metadata| {
        collector.push((payload.to_vec(), metadata.message_id.clone()))
    });

    let _subscription = chat.subscribe().unwrap();
    assert_eq!(peer.next_command().await, segment_command("SUB", "chat"));

    let (published, frame) = tokio::join!(
        chat.publish_with_message_id(b"hi", "m1"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$4\nchat\n$2\nm1\n$2\nhi\n");

    peer.send(delivery_frame("alice", "chat", "m2", 1, b"hello"))
        .await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    settle(&channel).await;
    assert_eq!(received.all(), vec![(b"hello".to_vec(), "m2".to_owned())]);

    channel.close().await;
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn connects_subscribes_publishes_receives_and_closes
