//! The writer packs the frames waiting for it into one `*N` frame (BATCH-01).
//!
//! Publishes started one after another, with no await between them, are all
//! handed to the writer before it runs, so it packs them together.

use std::time::Duration;

use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::segment::Segment;
use crate::tests::support::{
    Peer, PendingPublish, connected_channel, error_frame, publish_command, publish_now,
    receive_rate_limit, record_errors, segment_command, settle, start_publish, yield_until,
};

/// Holds the writer on a publish of its own, which goes out alone.
async fn hold_writer<'call>(chat: &'call Segment, peer: &Peer) -> PendingPublish<'call> {
    peer.hold_writes();
    let held = start_publish(chat, "h", "x").await;
    yield_until("the held write", || peer.has_held_write()).await;

    held
} // end fn hold_writer

async fn next_text(peer: &mut Peer) -> String {
    String::from_utf8(peer.next_frame().await).expect("commands in these tests are text")
} // end fn next_text

async fn await_all(publishes: Vec<PendingPublish<'_>>) {
    for publish in publishes {
        publish.await.expect("the publish is sent");
    }
} // end fn await_all

#[tokio::test(start_paused = true)]
async fn commands_waiting_for_the_writer_go_out_as_one_array_in_order() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let held = hold_writer(&chat, &peer).await;
    let _subscription = chat.subscribe().unwrap();
    let published = vec![
        start_publish(&chat, "id-1", "m1").await,
        start_publish(&chat, "id-2", "m2").await,
    ];

    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    assert_eq!(
        next_text(&mut peer).await,
        format!(
            "*3\n{}{}{}",
            segment_command("SUB", "chat"),
            publish_command("chat", "id-1", "m1"),
            publish_command("chat", "id-2", "m2")
        )
    );
    await_all(vec![held]).await;
    await_all(published).await;
} // end fn commands_waiting_for_the_writer_go_out_as_one_array_in_order

#[tokio::test(start_paused = true)]
async fn the_writer_packs_at_most_16_commands() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let message_ids: Vec<String> = (0..17).map(|index| format!("m-{index}")).collect();
    let held = hold_writer(&chat, &peer).await;
    let mut published = Vec::new();

    for message_id in &message_ids {
        published.push(start_publish(&chat, message_id, "x").await);
    }

    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    let batch = next_text(&mut peer).await;
    let expected: String = message_ids[..16]
        .iter()
        .map(|message_id| publish_command("chat", message_id, "x"))
        .collect();
    assert_eq!(batch, format!("*16\n{expected}"));
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "m-16", "x")
    );
    await_all(vec![held]).await;
    await_all(published).await;
} // end fn the_writer_packs_at_most_16_commands

/// A publish here is 26 bytes of framing plus its payload, so "*2\n" and
/// payloads of 32 740 and 32 741 bytes make exactly 65 536.
#[tokio::test(start_paused = true)]
async fn the_writer_packs_up_to_64_kib_with_the_array_header_and_no_further() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let (first, fits, overflows) = ("x".repeat(32_740), "x".repeat(32_741), "x".repeat(32_742));

    let held = hold_writer(&chat, &peer).await;
    let published = vec![
        start_publish(&chat, "a", &first).await,
        start_publish(&chat, "b", &fits).await,
    ];
    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    let batch = next_text(&mut peer).await;
    assert_eq!(batch.len(), 65_536);
    assert!(batch.starts_with("*2\n"));
    await_all(vec![held]).await;
    await_all(published).await;

    let held = hold_writer(&chat, &peer).await;
    let published = vec![
        start_publish(&chat, "c", &first).await,
        start_publish(&chat, "d", &overflows).await,
    ];
    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "c", &first)
    );
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "d", &overflows)
    );
    await_all(vec![held]).await;
    await_all(published).await;
} // end fn the_writer_packs_up_to_64_kib_with_the_array_header_and_no_further

#[tokio::test(start_paused = true)]
async fn a_command_larger_than_64_kib_goes_alone() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let large = "x".repeat(70 * 1024);
    let held = hold_writer(&chat, &peer).await;
    let published = vec![
        start_publish(&chat, "a", "x").await,
        start_publish(&chat, "b", &large).await,
        start_publish(&chat, "c", "x").await,
    ];

    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "a", "x")
    );
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "b", &large)
    );
    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "c", "x")
    );
    await_all(vec![held]).await;
    await_all(published).await;
} // end fn a_command_larger_than_64_kib_goes_alone

#[tokio::test(start_paused = true)]
async fn a_failed_write_fails_every_publish_in_its_batch() {
    let (channel, peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    peer.hold_writes();
    let published = vec![
        start_publish(&chat, "a", "x").await,
        start_publish(&chat, "b", "x").await,
        start_publish(&chat, "c", "x").await,
    ];
    yield_until("the held write", || peer.has_held_write()).await;

    peer.fail_writes();
    peer.release_writes();

    for publish in published {
        assert_eq!(
            publish.await,
            Err(CelerisError::DeliveryUnknown(
                "The socket failed while writing the publish, so it may or may not have been sent."
                    .to_owned()
            ))
        );
    }
} // end fn a_failed_write_fails_every_publish_in_its_batch

#[tokio::test(start_paused = true)]
async fn a_dropped_connection_leaves_every_publish_in_the_batch_unknown() {
    let (channel, peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    peer.hold_writes();
    let published = vec![
        start_publish(&chat, "a", "x").await,
        start_publish(&chat, "b", "x").await,
    ];
    yield_until("the held write", || peer.has_held_write()).await;

    drop(peer);

    for publish in published {
        assert_eq!(
            publish.await,
            Err(CelerisError::DeliveryUnknown(
                "The connection dropped while the socket was writing the publish, so it may or may not have been sent."
                    .to_owned()
            ))
        );
    }
} // end fn a_dropped_connection_leaves_every_publish_in_the_batch_unknown

#[tokio::test(start_paused = true)]
async fn cancelling_a_waiting_publish_takes_it_out_of_the_next_batch() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let held = hold_writer(&chat, &peer).await;
    let first = start_publish(&chat, "a", "x").await;
    drop(start_publish(&chat, "b", "x").await);
    let last = start_publish(&chat, "c", "x").await;

    peer.release_writes();

    assert_eq!(
        next_text(&mut peer).await,
        publish_command("chat", "h", "x")
    );
    assert_eq!(
        next_text(&mut peer).await,
        format!(
            "*2\n{}{}",
            publish_command("chat", "a", "x"),
            publish_command("chat", "c", "x")
        )
    );
    await_all(vec![held, first, last]).await;
} // end fn cancelling_a_waiting_publish_takes_it_out_of_the_next_batch

#[tokio::test(start_paused = true)]
async fn a_publish_refused_for_its_size_is_not_resent() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = record_errors(&channel).await;
    let chat = channel.segment("chat").unwrap();
    publish_now(&chat, &mut peer, "too-big", "big").await;
    publish_now(&chat, &mut peer, "fits", "ok").await;

    peer.send(error_frame(
        "MessageSizeLimitError",
        Some("PUB"),
        "Message size limit exceeded; payload size = 3 bytes; size limit = 1 KB",
        Some("too-big"),
    ))
    .await;
    receive_rate_limit(&channel, &mut peer).await;

    assert_eq!(
        peer.commands_within(Duration::from_secs(2)).await,
        vec![publish_command("chat", "fits", "ok")]
    );

    settle(&channel).await;
    let reported = errors.all();
    assert_eq!(reported.len(), 2);
    assert_eq!(
        reported[0],
        CelerisError::Server(ServerError {
            error_type: ServerErrorType::MessageSizeLimitError,
            sub_type: Some("PUB".to_owned()),
            message: "Message size limit exceeded; payload size = 3 bytes; size limit = 1 KB"
                .to_owned(),
            resource: ServerErrorResource::Text("too-big".to_owned()),
        })
    );
} // end fn a_publish_refused_for_its_size_is_not_resent
