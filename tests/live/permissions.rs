//! Per-segment permissions and the token reference (catalogue sections 6
//! and 12).

use useceleris_client::{
    CelerisError, ChannelState, ServerError, ServerErrorResource, ServerErrorType,
};

use crate::support::channels::{
    connected, publish_and_await, publish_text, segment, subscribe, subscribe_presence,
    unique_channel_reference,
};
use crate::support::collect::{assert_payloads, collect};
use crate::support::signer::{Claims, Replay, SegmentPermission, TokenPermission};
use crate::support::wait::{EVENT_WAIT, await_body, await_denial, await_presence, settle};

/// A token that can read "readonly", write "writeonly", and nothing else: no
/// other segment, and not "default" either.
fn segment_permissions() -> Claims {
    let permission = |segment_id: &str, read, write| SegmentPermission {
        segment_id: segment_id.to_owned(),
        read,
        write,
    };

    Claims {
        token_permission: Some(TokenPermission::Segments(vec![
            permission("readonly", true, false),
            permission("writeonly", false, true),
        ])),
        ..Claims::reference("limited")
    }
} // end fn segment_permissions

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lets_a_read_only_segment_receive_and_refuses_its_publish() {
    let reference = unique_channel_reference("perm-read");
    let publisher = connected(&reference, Claims::default()).await;
    let limited = connected(&reference, segment_permissions()).await;
    let readonly = segment(&limited, "readonly");
    let received = collect(&readonly);
    subscribe(&readonly);
    settle().await;

    let delivered = await_body(&readonly, "hello");
    publish_text(&publisher, "readonly", "hello").await;
    delivered.wait().await;

    let refused = await_denial(&limited, "PUB", "readonly");
    publish_text(&limited, "readonly", "refused").await;
    refused.wait().await;

    assert_payloads(&received, &["hello"]);
    assert_eq!(limited.state(), ChannelState::Connected);
} // end fn lets_a_read_only_segment_receive_and_refuses_its_publish

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lets_a_write_only_segment_publish_and_refuses_its_subscription() {
    let reference = unique_channel_reference("perm-write");
    let reader = connected(&reference, Claims::default()).await;
    let limited = connected(&reference, segment_permissions()).await;
    let writeonly = segment(&limited, "writeonly");
    let received = collect(&writeonly);
    subscribe(&segment(&reader, "writeonly"));
    let refused = await_denial(&limited, "SUB", "writeonly");
    subscribe(&writeonly);
    refused.wait().await;
    settle().await;

    publish_and_await(&limited, &reader, "writeonly", "from-limited").await;
    publish_text(&reader, "writeonly", "unheard").await;
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    assert_payloads(&received, &[]);
    assert_eq!(limited.state(), ChannelState::Connected);
} // end fn lets_a_write_only_segment_publish_and_refuses_its_subscription

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_a_write_only_publish_to_a_later_subscriber() {
    let reference = unique_channel_reference("perm-write-replay");
    let limited = connected(&reference, segment_permissions()).await;
    let writeonly = segment(&limited, "writeonly");
    let received = collect(&writeonly);
    // No connection is in the segment when the write-only token publishes.
    publish_text(&limited, "writeonly", "kept").await;
    settle().await;

    let reader = connected(&reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&reader, "writeonly");
    let replayed = await_body(&history, "kept");
    subscribe(&history);
    replayed.wait().await;

    assert_payloads(&received, &[]);
    assert_eq!(limited.state(), ChannelState::Connected);
} // end fn replays_a_write_only_publish_to_a_later_subscriber

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_presence_on_a_segment_without_read_access() {
    let limited = connected(
        &unique_channel_reference("perm-presence"),
        segment_permissions(),
    )
    .await;

    let refused_watch = await_denial(&limited, "PRES_SUB", "writeonly");
    subscribe_presence(&segment(&limited, "writeonly"));
    refused_watch.wait().await;

    let rejection = segment(&limited, "writeonly").presence_list(1, 10).await;

    // The first query of the channel has request id 1 (QUERY-01).
    assert!(
        matches!(
            &rejection,
            Err(CelerisError::Server(ServerError {
                error_type: ServerErrorType::PermissionDeniedError,
                sub_type: Some(sub_type),
                resource: ServerErrorResource::Text(request_id),
                ..
            })) if sub_type == "PRES_LIST" && request_id == "1"
        ),
        "got {rejection:?}"
    );

    let page = segment(&limited, "readonly")
        .presence_list(1, 10)
        .await
        .expect("the read-only list works");

    assert_eq!(page.connections, []);
} // end fn refuses_presence_on_a_segment_without_read_access

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_every_command_on_a_segment_the_token_does_not_list() {
    let limited = connected(
        &unique_channel_reference("perm-unlisted"),
        segment_permissions(),
    )
    .await;
    let secret = segment(&limited, "secret");

    let refused_join = await_denial(&limited, "SUB", "secret");
    subscribe(&secret);
    refused_join.wait().await;

    let refused_publish = await_denial(&limited, "PUB", "secret");
    publish_text(&limited, "secret", "x").await;
    refused_publish.wait().await;

    let refused_watch = await_denial(&limited, "PRES_SUB", "secret");
    subscribe_presence(&secret);
    refused_watch.wait().await;

    assert_eq!(limited.state(), ChannelState::Connected);
} // end fn refuses_every_command_on_a_segment_the_token_does_not_list

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gives_an_unlisted_default_segment_no_read_and_no_write_access() {
    let reference = unique_channel_reference("perm-default");
    let publisher = connected(&reference, Claims::default()).await;
    let limited = connected(&reference, segment_permissions()).await;
    let lobby = collect(&limited.default_segment());
    let readonly = segment(&limited, "readonly");
    let received = collect(&readonly);
    subscribe(&readonly);
    settle().await;

    publish_text(&publisher, "default", "unheard").await;
    publish_and_await(&publisher, &limited, "readonly", "control").await;

    let refused = await_denial(&limited, "PUB", "default");
    publish_text(&limited, "default", "refused").await;
    refused.wait().await;
    settle().await;

    assert_payloads(&lobby, &[]);
    assert_payloads(&received, &["control"]);
} // end fn gives_an_unlisted_default_segment_no_read_and_no_write_access

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shows_the_reference_claim_in_message_metadata_presence_events_and_presence_lists() {
    let reference = unique_channel_reference("token-reference");
    let watcher = connected(&reference, Claims::reference("watcher")).await;
    let alice = connected(&reference, Claims::reference("alice")).await;
    let room = segment(&watcher, "room");
    subscribe(&room);
    subscribe_presence(&room);
    settle().await;

    let joined = await_presence(
        &room,
        |event| event.joined && event.token_reference == "alice",
        "alice's join",
        EVENT_WAIT,
    );

    subscribe(&segment(&alice, "room"));
    joined.wait().await;

    let message = await_body(&room, "hi");
    publish_text(&alice, "room", "hi").await;

    assert_eq!(message.wait().await.metadata.token_reference, "alice");

    let page = room.presence_list(1, 10).await.expect("the presence list");
    let mut token_references: Vec<String> = page
        .connections
        .into_iter()
        .map(|connection| connection.token_reference)
        .collect();

    token_references.sort();

    assert_eq!(token_references, ["alice", "watcher"]);
} // end fn shows_the_reference_claim_in_message_metadata_presence_events_and_presence_lists
