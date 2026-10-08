use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::tests::support::{
    Recorder, connected_channel, delivery_frame, error_frame, error_recorder, notice_frame,
    record_message_ids, settle, state_recorder, wait_until,
};
use crate::values::{ChannelState, ServerNotice};

fn listener_panicked() -> CelerisError {
    CelerisError::Transport(
        "A listener callback panicked; the channel recovered and kept running.".to_owned(),
    )
} // end fn listener_panicked

#[tokio::test(start_paused = true)]
async fn every_server_error_type_and_an_unknown_type_surfaces_with_every_field() {
    let types = [
        ("ParserError", ServerErrorType::ParserError),
        ("SendError", ServerErrorType::SendError),
        (
            "PermissionDeniedError",
            ServerErrorType::PermissionDeniedError,
        ),
        ("RateLimitError", ServerErrorType::RateLimitError),
        (
            "MessageSizeLimitError",
            ServerErrorType::MessageSizeLimitError,
        ),
        ("InternalError", ServerErrorType::InternalError),
        (
            "SomeFutureError",
            ServerErrorType::Other("SomeFutureError".to_owned()),
        ),
    ];

    for (wire_type, error_type) in types {
        let (channel, mut peer, _server, _log) = connected_channel().await;
        let errors = error_recorder(&channel);

        peer.send(format!(
            "-Err\n+{wire_type}\n+SUB\n$13\nwhat happened\n*2\n+a\n:7\n"
        ))
        .await;
        wait_until("the server error", || errors.len() == 1).await;
        settle(&channel).await;

        let expected = CelerisError::Server(ServerError {
            error_type,
            sub_type: Some("SUB".to_owned()),
            message: "what happened".to_owned(),
            resource: ServerErrorResource::Array(vec![
                ServerErrorResource::Text("a".to_owned()),
                ServerErrorResource::Integer64(7),
            ]),
        });

        assert_eq!(errors.all(), vec![expected], "{wire_type}");
        assert_eq!(channel.state(), ChannelState::Connected, "{wire_type}");

        channel.close().await;
    }
} // end fn every_server_error_type_and_an_unknown_type_surfaces_with_every_field

#[tokio::test(start_paused = true)]
async fn server_error_frame_keeps_the_connection_up() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    // A denied publish succeeds locally; the error arrives afterwards.
    let chat = channel.segment("chat").unwrap();
    let (published, _frame) = tokio::join!(
        chat.publish_with_message_id(b"x", "m-1"),
        peer.next_command()
    );
    published.unwrap();

    peer.send(error_frame(
        "PermissionDeniedError",
        Some("PUB"),
        "denied",
        Some("chat"),
    ))
    .await;
    peer.send(error_frame("RateLimitError", None, "slow down", None))
        .await;
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(
        errors.all(),
        vec![
            CelerisError::Server(ServerError {
                error_type: ServerErrorType::PermissionDeniedError,
                sub_type: Some("PUB".to_owned()),
                message: "denied".to_owned(),
                resource: ServerErrorResource::Text("chat".to_owned()),
            }),
            CelerisError::Server(ServerError {
                error_type: ServerErrorType::RateLimitError,
                sub_type: None,
                message: "slow down".to_owned(),
                resource: ServerErrorResource::Null,
            }),
        ]
    );

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);
    assert_eq!(channel.state(), ChannelState::Connected);
    assert!(!server.has_pending_connection());

    channel.close().await;
} // end fn server_error_frame_keeps_the_connection_up

#[tokio::test(start_paused = true)]
async fn malformed_server_error_never_breaks_delivery() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    let mut frame = b"-Err\n+SendError\n$-1\n$6\nbad ".to_vec();
    frame.extend_from_slice(&[0xff, 0xfe]);
    frame.extend_from_slice(b"\n$-1\n");
    peer.send(frame).await;
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    // The server's text is passed on with the invalid bytes replaced.
    assert_eq!(
        errors.all(),
        vec![CelerisError::Server(ServerError {
            error_type: ServerErrorType::SendError,
            sub_type: None,
            message: "bad \u{FFFD}\u{FFFD}".to_owned(),
            resource: ServerErrorResource::Null,
        })]
    );

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn malformed_server_error_never_breaks_delivery

#[tokio::test(start_paused = true)]
async fn notices_are_delivered_raw_in_order_and_panicking_notice_listeners_are_contained() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let seen = Recorder::new();

    channel
        .events()
        .on_notice(|_notice| panic!("notice-secret"));

    let collector = seen.clone();
    let first = channel
        .events()
        .on_notice(move |notice| collector.push(("first", notice.clone())));

    let collector = seen.clone();
    channel
        .events()
        .on_notice(move |notice| collector.push(("second", notice.clone())));

    peer.send(notice_frame(7, "joined")).await;
    wait_until("the first notice", || seen.len() == 2).await;
    settle(&channel).await;
    first.remove();

    peer.send(notice_frame(8, "left")).await;
    peer.send(b"@SERVER_MSG\n:9\n$0\n\n".to_vec()).await;
    wait_until("the last notice", || seen.len() == 4).await;
    settle(&channel).await;

    let notice = |timestamp, payload: &[u8]| ServerNotice {
        timestamp,
        payload: payload.to_vec(),
    };

    assert_eq!(
        seen.all(),
        vec![
            ("first", notice(7, b"joined")),
            ("second", notice(7, b"joined")),
            ("second", notice(8, b"left")),
            ("second", notice(9, b"")),
        ]
    );

    assert_eq!(
        errors.all(),
        vec![
            listener_panicked(),
            listener_panicked(),
            listener_panicked()
        ]
    );
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn notices_are_delivered_raw_in_order_and_panicking_notice_listeners_are_contained

#[tokio::test(start_paused = true)]
async fn node_and_unknown_commands_are_skipped_without_a_report() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    settle(&channel).await;
    let errors = error_recorder(&channel);
    let states = state_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    // A ping from the server is answered by the socket, never routed.
    peer.send_ping().await;
    peer.send(b"@NODE_PUB\n+node-1\n$4\nbody\n".to_vec()).await;
    peer.send(b"@FUTURE_COMMAND\n+x\n:1\n".to_vec()).await;
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);
    assert_eq!(errors.all(), Vec::<CelerisError>::new());
    assert_eq!(states.all(), Vec::<ChannelState>::new());

    channel.close().await;
} // end fn node_and_unknown_commands_are_skipped_without_a_report

#[tokio::test(start_paused = true)]
async fn undecodable_frame_is_reported_and_the_next_frame_decodes() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    // An unknown command inside an array hides where the next entry starts,
    // so the whole frame is lost, its id-0 with it.
    let mut frame = b"*2\n@FUTURE\n+a\n".to_vec();
    frame.extend_from_slice(&delivery_frame("user", "chat", "id-0", 1, b"x"));
    peer.send(frame).await;
    peer.send(delivery_frame("user", "chat", "id-1", 2, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);

    assert_eq!(
        errors.all(),
        vec![CelerisError::Protocol {
            message: "Unknown command inside array has ambiguous boundaries.".to_owned(),
            field: "Command",
            offset: 3,
        }]
    );
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn undecodable_frame_is_reported_and_the_next_frame_decodes

#[tokio::test(start_paused = true)]
async fn text_websocket_frame_is_reported_as_a_protocol_error() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let delivered = record_message_ids(&channel.segment("chat").unwrap());

    peer.send_text("text").await;
    peer.send(delivery_frame("user", "chat", "id-1", 1, b"x"))
        .await;
    wait_until("the delivery", || delivered.len() == 1).await;
    settle(&channel).await;

    assert_eq!(
        errors.all(),
        vec![CelerisError::Protocol {
            message: "Expected a binary WebSocket message.".to_owned(),
            field: "Message",
            offset: 0,
        }]
    );
    assert_eq!(delivered.all(), vec!["id-1".to_owned()]);
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn text_websocket_frame_is_reported_as_a_protocol_error
