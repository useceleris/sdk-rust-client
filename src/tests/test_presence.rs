use std::time::Duration;

use futures_util::FutureExt;

use crate::errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
use crate::tests::support::{
    DialBehaviour, Recorder, client_with_server, connected_channel, error_frame, error_recorder,
    notice_frame, presence_frame, presence_list_frame, segment_command, settle, state_recorder,
    test_options, wait_until,
};
use crate::values::{ChannelState, PresenceConnection, PresenceEvent, PresencePage};

fn presence_list_command(segment_id: &str, page: i32, per_page: i32, request_id: &str) -> String {
    format!(
        "@PRES_LIST\n${}\n{segment_id}\n;{page}\n;{per_page}\n${}\n{request_id}\n",
        segment_id.len(),
        request_id.len()
    )
} // end fn presence_list_command

/// A one-entry first page for segment "chat" that answers `request_id`.
fn chat_page_frame(request_id: &str, total: i32) -> Vec<u8> {
    presence_list_frame(
        "chat",
        request_id,
        [total, 25, 1, 1, 1],
        &[("user", "connection-1", 123)],
    )
} // end fn chat_page_frame

/// An error frame that answers the presence query with `request_id`.
fn presence_error_frame(error_type: &str, request_id: &str) -> Vec<u8> {
    error_frame(error_type, Some("PRES_LIST"), "failed", Some(request_id))
} // end fn presence_error_frame

/// An error that the channel reports to its error listeners, sent last as a
/// control: once it arrives, every earlier frame was routed.
fn control_error_frame() -> Vec<u8> {
    error_frame("InternalError", Some("SUB"), "control", Some("chat"))
} // end fn control_error_frame

fn control_error() -> CelerisError {
    CelerisError::Server(ServerError {
        error_type: ServerErrorType::InternalError,
        sub_type: Some("SUB".to_owned()),
        message: "control".to_owned(),
        resource: ServerErrorResource::Text("chat".to_owned()),
    })
} // end fn control_error

fn presence_event(connection_id: &str, joined: bool, timestamp: i64) -> PresenceEvent {
    PresenceEvent {
        segment_id: "chat".to_owned(),
        token_reference: "user".to_owned(),
        connection_id: connection_id.to_owned(),
        joined,
        timestamp,
    }
} // end fn presence_event

#[tokio::test(start_paused = true)]
async fn presence_interests_share_one_count_and_send_golden_bytes() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let first = channel
        .segment("chat")
        .unwrap()
        .subscribe_presence()
        .unwrap();
    let second = channel
        .segment("chat")
        .unwrap()
        .subscribe_presence()
        .unwrap();
    assert_eq!(peer.next_frame().await, b"@PRES_SUB\n$4\nchat\n");

    first.cancel();
    first.cancel();
    second.cancel();
    assert_eq!(peer.next_frame().await, b"@PRES_UNSUB\n$4\nchat\n");

    // The default segment takes presence commands too.
    channel
        .default_segment()
        .subscribe_presence()
        .unwrap()
        .cancel();
    assert_eq!(peer.next_frame().await, b"@PRES_SUB\n$7\ndefault\n");
    assert_eq!(peer.next_frame().await, b"@PRES_UNSUB\n$7\ndefault\n");

    channel.close().await;
} // end fn presence_interests_share_one_count_and_send_golden_bytes

#[tokio::test(start_paused = true)]
async fn presence_query_resolves_with_raw_metadata() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    let (page, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_frame().await,
            b"@PRES_LIST\n$4\nchat\n;1\n;25\n$1\n1\n"
        );

        peer.send(presence_list_frame(
            "chat",
            "1",
            [2, 25, 1, 1, 2],
            &[("user", "connection-1", 123), ("user", "connection-2", 456)],
        ))
        .await;
    });

    let connection = |connection_id: &str, timestamp| PresenceConnection {
        token_reference: "user".to_owned(),
        connection_id: connection_id.to_owned(),
        timestamp,
    };

    assert_eq!(
        page,
        Ok(PresencePage {
            segment_id: "chat".to_owned(),
            total: 2,
            per_page: 25,
            current_page: 1,
            from: 1,
            to: 2,
            connections: vec![
                connection("connection-1", 123),
                connection("connection-2", 456)
            ],
        })
    );

    // Past the last page, from is larger than to.
    let (page, ()) = tokio::join!(chat.presence_list(2, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 2, 25, "2")
        );
        peer.send(presence_list_frame("chat", "2", [1, 25, 2, 26, 1], &[]))
            .await;
    });

    assert_eq!(
        page,
        Ok(PresencePage {
            segment_id: "chat".to_owned(),
            total: 1,
            per_page: 25,
            current_page: 2,
            from: 26,
            to: 1,
            connections: Vec::new(),
        })
    );

    channel.close().await;
} // end fn presence_query_resolves_with_raw_metadata

#[tokio::test(start_paused = true)]
async fn one_presence_query_at_a_time() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();
    let other = channel.segment("other").unwrap();

    let (first, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "1")
        );

        assert_eq!(
            other.presence_list(1, 25).now_or_never(),
            Some(Err(CelerisError::OperationInProgress(
                "A presence query is already in flight; wait for it to settle before starting another."
                    .to_owned()
            )))
        );

        peer.send(chat_page_frame("1", 1)).await;
    });

    assert_eq!(first.unwrap().segment_id, "chat");

    channel.close().await;
} // end fn one_presence_query_at_a_time

#[tokio::test(start_paused = true)]
async fn presence_response_is_matched_by_request_id_alone() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    // No query waits for this one.
    peer.send(chat_page_frame("9", 1)).await;

    let (page, ()) = tokio::join!(chat.presence_list(2, 50), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 2, 50, "1")
        );
        peer.send(chat_page_frame("0", 1)).await;
        peer.send(chat_page_frame("10", 1)).await;

        // The id alone decides; the other fields are the server's to report.
        peer.send(presence_list_frame("chat", "1", [1, 25, 7, 1, 1], &[]))
            .await;
    });

    assert_eq!(page.unwrap().current_page, 7);
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn presence_response_is_matched_by_request_id_alone

#[tokio::test(start_paused = true)]
async fn presence_error_naming_the_pending_query_rejects_it_at_once_and_is_no_error_event() {
    for error_type in ["InternalError", "PermissionDeniedError"] {
        let (channel, mut peer, _server, _log) = connected_channel().await;
        let errors = error_recorder(&channel);
        let chat = channel.segment("chat").unwrap();

        let (rejected, ()) = tokio::join!(chat.presence_list(1, 25), async {
            assert_eq!(
                peer.next_command().await,
                presence_list_command("chat", 1, 25, "1")
            );
            peer.send(presence_error_frame(error_type, "1")).await;
        });

        assert_eq!(
            rejected,
            Err(CelerisError::Server(ServerError {
                error_type: ServerErrorType::from_wire(error_type.to_owned()),
                sub_type: Some("PRES_LIST".to_owned()),
                message: "failed".to_owned(),
                resource: ServerErrorResource::Text("1".to_owned()),
            }))
        );

        // The connection stays, and the next query works.
        let (next, ()) = tokio::join!(chat.presence_list(1, 25), async {
            assert_eq!(
                peer.next_command().await,
                presence_list_command("chat", 1, 25, "2")
            );
            peer.send(chat_page_frame("2", 4)).await;
        });

        assert_eq!(next.unwrap().total, 4);

        peer.send(control_error_frame()).await;
        wait_until("the control error", || errors.len() == 1).await;
        settle(&channel).await;

        assert_eq!(errors.all(), vec![control_error()], "{error_type}");
        assert_eq!(channel.state(), ChannelState::Connected);

        channel.close().await;
    }
} // end fn presence_error_naming_the_pending_query_rejects_it_at_once_and_is_no_error_event

#[tokio::test(start_paused = true)]
async fn presence_error_for_any_other_request_id_is_dropped() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let chat = channel.segment("chat").unwrap();

    // No query waits for this one. The control error shows that it was
    // routed before the query takes request id 1.
    peer.send(presence_error_frame("InternalError", "1")).await;
    peer.send(control_error_frame()).await;
    wait_until("the first control error", || errors.len() == 1).await;

    let (page, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "1")
        );
        peer.send(presence_error_frame("InternalError", "0")).await;
        peer.send(chat_page_frame("1", 5)).await;
    });

    assert_eq!(page.unwrap().total, 5);

    peer.send(control_error_frame()).await;
    wait_until("the second control error", || errors.len() == 2).await;
    settle(&channel).await;

    assert_eq!(errors.all(), vec![control_error(), control_error()]);

    channel.close().await;
} // end fn presence_error_for_any_other_request_id_is_dropped

#[tokio::test(start_paused = true)]
async fn presence_timeout_frees_the_slot_keeps_the_connection_and_drops_the_late_reply() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    // The connect's own state changes reach only listeners registered before
    // their turn, so they are delivered first.
    settle(&channel).await;
    let states = state_recorder(&channel);
    let chat = channel.segment("chat").unwrap();

    let query = chat.presence_list(1, 25);
    tokio::pin!(query);

    let early = tokio::time::timeout(Duration::from_millis(9_999), &mut query).await;
    assert!(early.is_err(), "the query settled before its timeout");

    assert_eq!(
        query.await,
        Err(CelerisError::Timeout(
            "Presence query timed out after 10000 ms.".to_owned()
        ))
    );

    assert_eq!(
        peer.next_command().await,
        presence_list_command("chat", 1, 25, "1")
    );
    assert_eq!(channel.state(), ChannelState::Connected);

    // The late reply and the late error carry the old request id.
    let (next, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "2")
        );
        peer.send(chat_page_frame("1", 1)).await;
        peer.send(presence_error_frame("InternalError", "1")).await;
        peer.send(chat_page_frame("2", 7)).await;
    });

    assert_eq!(next.unwrap().total, 7);

    peer.send(control_error_frame()).await;
    wait_until("the control error", || errors.len() == 1).await;
    channel.close().await;
    settle(&channel).await;

    assert_eq!(errors.all(), vec![control_error()]);

    // Nothing changed the state until close.
    assert_eq!(
        states.all(),
        vec![ChannelState::Closing, ChannelState::Closed]
    );
} // end fn presence_timeout_frees_the_slot_keeps_the_connection_and_drops_the_late_reply

#[tokio::test(start_paused = true)]
async fn dropping_a_sent_presence_query_frees_the_slot_and_keeps_the_connection() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    // The connect's own state changes reach only listeners registered before
    // their turn, so they are delivered first.
    settle(&channel).await;
    let states = state_recorder(&channel);
    let chat = channel.segment("chat").unwrap();
    let mut query = Box::pin(chat.presence_list(1, 25));

    tokio::select! {
        _ = query.as_mut() => panic!("the query settled without a reply"),
        frame = peer.next_command() => {
            assert_eq!(frame, presence_list_command("chat", 1, 25, "1"));
        }
    }

    drop(query);

    let (next, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "2")
        );
        peer.send(chat_page_frame("1", 1)).await;
        peer.send(chat_page_frame("2", 3)).await;
    });

    assert_eq!(next.unwrap().total, 3);
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
    settle(&channel).await;

    assert_eq!(
        states.all(),
        vec![ChannelState::Closing, ChannelState::Closed]
    );
} // end fn dropping_a_sent_presence_query_frees_the_slot_and_keeps_the_connection

#[tokio::test(start_paused = true)]
async fn dropping_a_presence_query_before_it_is_sent_sends_nothing() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    drop(chat.presence_list(1, 25));

    // The next query is the first frame, and it takes the first request id.
    let (page, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "1")
        );
        peer.send(chat_page_frame("1", 2)).await;
    });

    assert_eq!(page.unwrap().total, 2);

    channel.close().await;
} // end fn dropping_a_presence_query_before_it_is_sent_sends_nothing

#[tokio::test(start_paused = true)]
async fn presence_query_is_refused_while_not_connected() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let idle = client.channel("test-channel").unwrap();

    assert_eq!(
        idle.segment("chat").unwrap().presence_list(1, 25).await,
        Err(CelerisError::NotConnected(
            "Channel is not connected; it is idle.".to_owned()
        ))
    );

    assert!(!server.has_pending_connection());

    let (channel, peer, server, _log) = connected_channel().await;
    server.set_behaviour(DialBehaviour::Hang);
    peer.close().await;
    wait_until("the connection loss", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;

    assert_eq!(
        channel.segment("chat").unwrap().presence_list(1, 25).await,
        Err(CelerisError::NotConnected(
            "Channel is not connected; it is reconnecting.".to_owned()
        ))
    );

    channel.close().await;
} // end fn presence_query_is_refused_while_not_connected

#[tokio::test(start_paused = true)]
async fn pending_presence_query_is_rejected_on_connection_loss_and_on_close() {
    let (channel, mut peer, server, _log) = connected_channel().await;
    server.set_behaviour(DialBehaviour::Hang);
    let chat = channel.segment("chat").unwrap();

    let (lost, ()) = tokio::join!(chat.presence_list(1, 25), async move {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "1")
        );
        peer.close().await;
    });

    assert_eq!(
        lost,
        Err(CelerisError::Transport(
            "Connection lost during the presence query; query again once the channel reconnects."
                .to_owned()
        ))
    );

    channel.close().await;

    let (channel, mut peer, _server, _log) = connected_channel().await;
    let chat = channel.segment("chat").unwrap();

    let (closed, ()) = tokio::join!(chat.presence_list(1, 25), async {
        assert_eq!(
            peer.next_command().await,
            presence_list_command("chat", 1, 25, "1")
        );
        channel.close().await;
    });

    assert_eq!(
        closed,
        Err(CelerisError::Cancelled(
            "Channel closed while the presence query was pending.".to_owned()
        ))
    );
} // end fn pending_presence_query_is_rejected_on_connection_loss_and_on_close

/// Go times this query out, since its listener blocks the receive loop. Here a
/// listener spawns a task that queries, and the query settles.
#[tokio::test(start_paused = true)]
async fn presence_query_started_inside_a_listener_settles_when_its_response_arrives() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let results = Recorder::new();
    let chat = channel.segment("chat").unwrap();
    let collector = results.clone();

    channel.events().on_notice(move |_notice| {
        let chat = chat.clone();
        let collector = collector.clone();

        tokio::spawn(async move { collector.push(chat.presence_list(1, 25).await) });
    });

    peer.send(notice_frame(1, "hello")).await;
    assert_eq!(
        peer.next_command().await,
        presence_list_command("chat", 1, 25, "1")
    );

    peer.send(chat_page_frame("1", 3)).await;
    wait_until("the query result", || results.len() == 1).await;

    let page = results.all().remove(0).unwrap();
    assert_eq!((page.segment_id.as_str(), page.total), ("chat", 3));

    channel.close().await;
} // end fn presence_query_started_inside_a_listener_settles_when_its_response_arrives

#[tokio::test(start_paused = true)]
async fn presence_events_go_only_to_their_segment_and_panicking_listeners_are_contained() {
    let (channel, mut peer, _server, _log) = connected_channel().await;
    let errors = error_recorder(&channel);
    let chat = Recorder::new();
    let lobby = Recorder::new();
    let order = Recorder::new();

    channel
        .segment("chat")
        .unwrap()
        .on_presence(|_event| panic!("presence-secret"));

    let collector = chat.clone();
    channel
        .segment("chat")
        .unwrap()
        .on_presence(move |event| collector.push(event.clone()));

    let collector = order.clone();
    channel
        .segment("chat")
        .unwrap()
        .on_presence(move |_event| collector.push("after"));

    let collector = lobby.clone();
    channel
        .segment("lobby")
        .unwrap()
        .on_presence(move |event| collector.push(event.clone()));

    peer.send(presence_frame("chat", "user", "connection-1", true, 7))
        .await;

    // No listener watches this segment.
    peer.send(presence_frame("unwatched", "user", "connection-2", true, 8))
        .await;
    peer.send(presence_frame("chat", "user", "connection-1", false, 9))
        .await;
    wait_until("both chat events", || chat.len() == 2).await;
    settle(&channel).await;

    assert_eq!(
        chat.all(),
        vec![
            presence_event("connection-1", true, 7),
            presence_event("connection-1", false, 9)
        ]
    );

    assert_eq!(lobby.len(), 0);
    assert_eq!(order.all(), vec!["after", "after"]);

    let panicked = CelerisError::Transport(
        "A listener callback panicked; the channel recovered and kept running.".to_owned(),
    );

    assert_eq!(errors.all(), vec![panicked.clone(), panicked]);
    assert_eq!(channel.state(), ChannelState::Connected);

    // The unwatched event cost nothing: the channel still takes commands.
    let _watching = channel
        .segment("chat")
        .unwrap()
        .subscribe_presence()
        .unwrap();
    assert_eq!(
        peer.next_command().await,
        segment_command("PRES_SUB", "chat")
    );

    channel.close().await;
} // end fn presence_events_go_only_to_their_segment_and_panicking_listeners_are_contained
