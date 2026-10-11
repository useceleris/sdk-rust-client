use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use futures_util::FutureExt;

use crate::constants::SDK_VERSION;
use crate::errors::CelerisError;
use crate::random::message_id_prefix;
use crate::tests::support::{
    DialBehaviour, Recorder, client_with_server, connected_channel, delivery_frame, error_recorder,
    fill_pipe, settle, state_recorder, test_options, wait_until,
};
use crate::values::ChannelState;

const COMMAND_TOO_LARGE: &str = "Encoded command exceeds 2 MiB. That is the most the server accepts on any plan; send a smaller payload.";

fn not_connected(state: &str) -> CelerisError {
    CelerisError::NotConnected(format!("Channel is not connected; it is {state}."))
} // end fn not_connected

#[tokio::test(start_paused = true)]
async fn publish_resolves_on_local_acceptance_with_exact_bytes_and_a_generated_id() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.default_segment();

    let (published, frame) = tokio::join!(lobby.publish(b"hi"), peer.next_command());
    published.unwrap();

    // "@PUB\n$7\ndefault\n", the length of the id, the SDK's prefix, 32
    // lowercase hex digits, "\n$2\nhi\n".
    let id_prefix = message_id_prefix("rust", SDK_VERSION);
    let prefix = format!("@PUB\n$7\ndefault\n${}\n{id_prefix}", id_prefix.len() + 32);
    let suffix = "\n$2\nhi\n";
    assert_eq!(frame.len(), prefix.len() + 32 + suffix.len(), "{frame:?}");
    assert!(frame.starts_with(prefix.as_str()), "{frame:?}");
    assert!(frame.ends_with(suffix), "{frame:?}");

    let generated = frame[prefix.len()..prefix.len() + 32].to_owned();

    assert!(
        generated
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{generated:?}"
    );

    let (published, frame) = tokio::join!(lobby.publish(b"hi"), peer.next_command());
    published.unwrap();
    assert_ne!(frame[prefix.len()..prefix.len() + 32], generated);

    let chat = channel.segment("chat").unwrap();
    let (published, frame) = tokio::join!(
        chat.publish_with_message_id(b"yo", "m-1"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$4\nchat\n$3\nm-1\n$2\nyo\n");

    channel.close().await;
} // end fn publish_resolves_on_local_acceptance_with_exact_bytes_and_a_generated_id

#[tokio::test(start_paused = true)]
async fn publish_before_during_and_after_a_failed_first_connect_is_refused_at_once() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let chat = channel.segment("chat").unwrap();

    assert_eq!(
        chat.publish(b"before connect").now_or_never(),
        Some(Err(not_connected("idle")))
    );

    server.set_behaviour(DialBehaviour::Hang);

    let connecting = tokio::spawn({
        let channel = channel.clone();

        async move { channel.connect().await }
    });

    wait_until("the first connect to start", || {
        channel.state() == ChannelState::Connecting
    })
    .await;

    assert_eq!(
        chat.publish(b"during connect").now_or_never(),
        Some(Err(not_connected("connecting")))
    );

    assert_eq!(
        connecting.await.unwrap(),
        Err(CelerisError::Timeout(
            "Connection attempt timed out after 15000 ms.".to_owned()
        ))
    );

    assert_eq!(channel.state(), ChannelState::Failed);

    assert_eq!(
        chat.publish(b"after failed").now_or_never(),
        Some(Err(not_connected("failed")))
    );

    channel.close().await;

    assert_eq!(
        chat.publish(b"after close").now_or_never(),
        Some(Err(not_connected("closed")))
    );

    // The one dial hung, so no socket ever existed to write to.
    assert_eq!(server.urls.len(), 1);
    assert!(!server.has_pending_connection());
} // end fn publish_before_during_and_after_a_failed_first_connect_is_refused_at_once

#[tokio::test(start_paused = true)]
async fn publish_refuses_a_dropped_future_and_invalid_input_before_writing() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let lobby = channel.default_segment();

    // A future dropped before its first poll is the pre-cancelled signal: it
    // never starts.
    drop(lobby.publish_with_message_id(b"dropped", "m-0"));

    assert_eq!(
        lobby.publish_with_message_id(b"x", "").await,
        Err(CelerisError::Configuration(
            "Invalid command. message_id: Must not be empty.".to_owned()
        ))
    );

    assert_eq!(
        lobby.publish_with_message_id(b"x", "bad\nid").await,
        Err(CelerisError::Configuration(
            "Invalid command. message_id: Must not contain CR or LF.".to_owned()
        ))
    );

    assert_eq!(
        lobby.publish_with_message_id(b"x", "bad\rid").await,
        Err(CelerisError::Configuration(
            "Invalid command. message_id: Must not contain CR or LF.".to_owned()
        ))
    );

    assert_eq!(
        lobby.publish(vec![0_u8; 2 * 1024 * 1024]).await,
        Err(CelerisError::Configuration(COMMAND_TOO_LARGE.to_owned()))
    );

    // The first frame on the wire is the next valid publish: an empty payload.
    let (published, frame) = tokio::join!(
        lobby.publish_with_message_id(b"", "m-1"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$7\ndefault\n$3\nm-1\n$0\n\n");

    channel.close().await;
} // end fn publish_refuses_a_dropped_future_and_invalid_input_before_writing

#[tokio::test(start_paused = true)]
async fn publishes_wait_behind_a_full_writer_and_are_sent_when_it_drains() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    fill_pipe(&channel).await;
    let lobby = channel.default_segment();
    let mut results = Vec::new();

    // The first is stuck in the socket. With it, 64 fill the writer and 64 more
    // wait in the publish queue.
    for index in 0..128 {
        let lobby = lobby.clone();

        results.push(tokio::spawn(async move {
            lobby
                .publish_with_message_id(b"x", &format!("m-{index}"))
                .await
        }));
    }

    tokio::time::sleep(Duration::from_millis(1)).await;

    assert_eq!(
        lobby.publish_with_message_id(b"x", "m-128").now_or_never(),
        Some(Err(CelerisError::Backpressure(
            "The publish queue is full (size 64). Retry once some publishes have gone out."
                .to_owned()
        )))
    );

    let settled = results.iter().filter(|result| result.is_finished()).count();
    assert_eq!(settled, 0);

    // The peer reads, so the writer drains, in publish order.
    assert_eq!(peer.next_frame().await.len(), 2_097_152);
    assert_eq!(peer.next_frame().await.len(), 2_097_124);

    for index in 0..128 {
        let message_id = format!("m-{index}");

        assert_eq!(
            peer.next_command().await,
            format!(
                "@PUB\n$7\ndefault\n${}\n{message_id}\n$1\nx\n",
                message_id.len()
            )
        );
    }

    for result in results {
        assert_eq!(result.await.unwrap(), Ok(()));
    }

    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn publishes_wait_behind_a_full_writer_and_are_sent_when_it_drains

/// The reference keeps its socket after a send that throws. Here a write that
/// the socket refuses leaves the server's view of the connection unknown, so
/// the socket is replaced and the channel recovers: the publish still settles
/// with DeliveryUnknown, no error is reported, and it is never resent.
#[tokio::test(start_paused = true)]
async fn native_send_failure_gives_delivery_unknown_and_the_channel_recovers() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    // The connect's own state changes reach only listeners registered before
    // their turn, so they are delivered first.
    settle(&channel).await;
    let states = state_recorder(&channel);
    let chat = channel.segment("chat").unwrap();

    // A listener that blocks holds the reader before its next message, so the
    // reader does not see the peer go away; only the writer does.
    let (release, gate) = mpsc::channel::<()>();
    let gate = Mutex::new(gate);
    let started = Recorder::new();
    let collector = started.clone();

    chat.on_message(move |_payload, metadata| {
        collector.push(metadata.message_id.clone());
        let _ = gate.lock().unwrap().recv();
    });

    // Time stands still while the listener blocks, so this test waits by
    // yielding, not by sleeping.
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;

    while started.len() == 0 {
        tokio::task::yield_now().await;
    }

    // The reader reads id-2 and waits for the listener.
    peer.send(delivery_frame("user", "chat", "id-2", 2, b"x"))
        .await;

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    drop(peer);

    assert_eq!(
        chat.publish(b"x").await,
        Err(CelerisError::DeliveryUnknown(
            "The socket failed while writing the publish, so it may or may not have been sent."
                .to_owned()
        ))
    );

    release.send(()).unwrap();
    let mut restored = server.accept().await;
    wait_until("the reconnect", || {
        channel.state() == ChannelState::Connected
    })
    .await;
    settle(&channel).await;

    assert_eq!(
        states.all(),
        vec![ChannelState::Reconnecting, ChannelState::Connected]
    );
    assert_eq!(errors.all(), Vec::<CelerisError>::new());
    assert_eq!(started.all(), vec!["id-1".to_owned()]);

    // The first frame on the new socket is the next publish: the failed one is
    // never resent.
    let (published, frame) = tokio::join!(
        chat.publish_with_message_id(b"after", "m-1"),
        restored.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$4\nchat\n$3\nm-1\n$5\nafter\n");

    channel.close().await;
} // end fn native_send_failure_gives_delivery_unknown_and_the_channel_recovers

#[tokio::test(start_paused = true)]
async fn command_at_the_2_mib_limit_is_sent_and_one_byte_more_is_refused_before_a_write() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let segment = channel.segment("s").unwrap();

    // "@PUB\n$1\ns\n$1\nm\n$2097127\n" and the last LF are 25 bytes.
    let (published, frame) = tokio::join!(
        segment.publish_with_message_id(vec![7_u8; 2_097_127], "m"),
        peer.next_frame()
    );
    published.unwrap();
    assert_eq!(frame.len(), 2 * 1024 * 1024);
    assert_eq!(&frame[..24], b"@PUB\n$1\ns\n$1\nm\n$2097127\n");
    assert_eq!(frame.iter().filter(|&&byte| byte == 7).count(), 2_097_127);
    assert_eq!(frame.last(), Some(&b'\n'));

    assert_eq!(
        segment
            .publish_with_message_id(vec![7_u8; 2_097_128], "m")
            .now_or_never(),
        Some(Err(CelerisError::Configuration(
            COMMAND_TOO_LARGE.to_owned()
        )))
    );

    // The refused command never reached the socket.
    let (published, frame) = tokio::join!(
        segment.publish_with_message_id(b"after", "m-1"),
        peer.next_command()
    );
    published.unwrap();
    assert_eq!(frame, "@PUB\n$1\ns\n$3\nm-1\n$5\nafter\n");
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn command_at_the_2_mib_limit_is_sent_and_one_byte_more_is_refused_before_a_write

#[tokio::test(start_paused = true)]
async fn binary_payloads_all_byte_values_and_newline_bytes_are_delivered_unchanged() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let mut payload: Vec<u8> = (0..=255).collect();
    payload.extend_from_slice(b"\n\r\n\n");

    let received = Recorder::new();
    let collector = received.clone();
    chat.on_message(move |payload, _metadata| collector.push(payload.to_vec()));

    let (published, frame) = tokio::join!(
        chat.publish_with_message_id(&payload, "bin-1"),
        peer.next_frame()
    );
    published.unwrap();

    let mut expected = b"@PUB\n$4\nchat\n$5\nbin-1\n$260\n".to_vec();
    expected.extend_from_slice(&payload);
    expected.push(b'\n');
    assert_eq!(frame, expected);

    peer.send(delivery_frame("user", "chat", "bin-2", 1, &payload))
        .await;
    peer.send(delivery_frame("user", "chat", "bin-3", 2, b"\n"))
        .await;
    wait_until("both deliveries", || received.len() == 2).await;
    settle(&channel).await;

    assert_eq!(received.all(), vec![payload, b"\n".to_vec()]);

    channel.close().await;
} // end fn binary_payloads_all_byte_values_and_newline_bytes_are_delivered_unchanged
