//! Cross-node (catalogue sections 8 and 13). The second connection opens
//! through CELERIS_WS_URL_PEER, a gateway that routes to a different server
//! node. Without it, these tests return early.

use std::time::Duration;

use tokio::time::sleep;

use crate::support::channels::{
    connected, connected_at, publish_all, publish_paced, publish_text, segment, subscribe,
    subscribe_presence, unique_channel_reference,
};
use crate::support::collect::{assert_payloads, collect, is_generated_message_id, numbered};
use crate::support::environment::peer_websocket_url;
use crate::support::signer::{Claims, Replay};
use crate::support::wait::{await_body, await_presence};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fans_out_publishes_across_nodes() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xnode");
    let primary = connected(&reference, Claims::default()).await;
    let secondary = connected_at(peer, &reference, Claims::default()).await;
    let chat = segment(&secondary, "chat");
    subscribe(&chat);
    sleep(Duration::from_secs(2)).await;

    let arrived = await_body(&chat, "across");
    publish_text(&primary, "chat", "across").await;
    let message = arrived.wait().await;

    assert!(
        is_generated_message_id(&message.metadata.message_id),
        "message id {:?}",
        message.metadata.message_id
    );
} // end fn fans_out_publishes_across_nodes

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reports_consistent_presence_across_nodes() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xpres");
    let primary = connected(&reference, Claims::default()).await;
    let secondary = connected_at(peer, &reference, Claims::default()).await;
    subscribe(&segment(&primary, "room"));
    subscribe(&segment(&secondary, "room"));
    sleep(Duration::from_secs(3)).await;

    let from_primary = segment(&primary, "room")
        .presence_list(1, 25)
        .await
        .expect("the primary's list");

    let from_secondary = segment(&secondary, "room")
        .presence_list(1, 25)
        .await
        .expect("the secondary's list");

    let connection_ids = |page: &useceleris_client::PresencePage| {
        let mut ids: Vec<String> = page
            .connections
            .iter()
            .map(|connection| connection.connection_id.clone())
            .collect();

        ids.sort();
        ids
    };

    assert_eq!((from_primary.total, from_secondary.total), (2, 2));
    assert_eq!(
        connection_ids(&from_primary),
        connection_ids(&from_secondary)
    );
} // end fn reports_consistent_presence_across_nodes

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_a_presence_join_from_another_node() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xpres-join");
    let watcher = connected(&reference, Claims::default()).await;
    let room = segment(&watcher, "room");
    subscribe_presence(&room);
    sleep(Duration::from_secs(2)).await;
    let joiner = connected_at(peer, &reference, Claims::default()).await;

    let joined = await_presence(
        &room,
        |event| event.joined,
        "the cross-node join",
        Duration::from_secs(20),
    );
    subscribe(&segment(&joiner, "room"));
    let join = joined.wait().await;

    assert_eq!(join.segment_id, "room");
} // end fn delivers_a_presence_join_from_another_node

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_a_presence_leave_from_another_node() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xpres-leave");
    let watcher = connected(&reference, Claims::default()).await;
    let room = segment(&watcher, "room");
    subscribe_presence(&room);
    sleep(Duration::from_secs(2)).await;
    let leaver = connected_at(peer, &reference, Claims::default()).await;

    let joined = await_presence(
        &room,
        |event| event.joined,
        "the cross-node join",
        Duration::from_secs(20),
    );
    subscribe(&segment(&leaver, "room"));
    let join = joined.wait().await;

    assert_eq!(join.segment_id, "room");

    let connection_id = join.connection_id.clone();

    let left = await_presence(
        &room,
        move |event| !event.joined && event.connection_id == connection_id,
        "the cross-node leave",
        Duration::from_secs(20),
    );

    leaver.close().await;
    left.wait().await;
} // end fn delivers_a_presence_leave_from_another_node

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replays_history_published_on_another_node() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xreplay");
    let publisher = connected(&reference, Claims::default()).await;
    publish_all(&publisher, "history", &["h1", "h2", "h3"]).await;
    sleep(Duration::from_secs(2)).await;

    let receiver = connected_at(peer, &reference, Claims::replay(Replay::Backlog)).await;
    let history = segment(&receiver, "history");
    let replayed = collect(&history);
    let last = await_body(&history, "h3");
    subscribe(&history);
    last.wait().await;
    sleep(Duration::from_secs(2)).await;

    assert_payloads(&replayed, &["h1", "h2", "h3"]);
} // end fn replays_history_published_on_another_node

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_one_origins_order_across_nodes() {
    let Some(peer) = peer_websocket_url() else {
        return;
    };

    let reference = unique_channel_reference("xorder");
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = connected_at(peer, &reference, Claims::default()).await;
    let chat = segment(&receiver, "chat");
    let received = collect(&chat);
    subscribe(&chat);
    sleep(Duration::from_secs(2)).await;
    let bodies = numbered("o", 30);
    let bodies: Vec<&str> = bodies.iter().map(String::as_str).collect();

    let last = await_body(&chat, "o29");
    publish_paced(&publisher, "chat", &bodies).await;
    last.wait().await;
    sleep(Duration::from_secs(2)).await;

    assert_payloads(&received, &bodies);
} // end fn keeps_one_origins_order_across_nodes
