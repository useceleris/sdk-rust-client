//! Presence (catalogue section 7).

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::{Instant, sleep};
use useceleris_client::{
    CelerisError, ChannelState, PresencePage, Segment, ServerError, ServerErrorResource,
    ServerErrorType,
};

use crate::support::channels::{
    Opened, connected, publish_text, segment, subscribe, subscribe_presence,
    unique_channel_reference,
};
use crate::support::collect::{Recorder, record_presence};
use crate::support::signer::Claims;
use crate::support::wait::{EVENT_WAIT, await_body, await_presence, eventually, settle};

/// Checks that the first page of 100 lists exactly these connections, each
/// with the token reference `user_1`.
async fn assert_listed(segment: &Segment, connection_ids: &[String]) {
    let page = segment
        .presence_list(1, 100)
        .await
        .expect("the presence list");

    for connection in &page.connections {
        assert_eq!(
            connection.token_reference, "user_1",
            "listed {connection:?}"
        );
    }

    let mut listed: Vec<&str> = page
        .connections
        .iter()
        .map(|connection| connection.connection_id.as_str())
        .collect();

    let mut expected: Vec<&str> = connection_ids.iter().map(String::as_str).collect();
    listed.sort_unstable();
    expected.sort_unstable();

    assert_eq!(usize::try_from(page.total), Ok(expected.len()), "the total");
    assert_eq!(listed, expected);
} // end fn assert_listed

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_typed_join_and_leave_notifications_to_watchers() {
    let reference = unique_channel_reference("watch");
    let watcher = connected(&reference, Claims::default()).await;
    let watched = segment(&watcher, "room");
    subscribe_presence(&watched);
    settle().await;

    let joined = await_presence(&watched, |event| event.joined, "a join", EVENT_WAIT);
    let actor = connected(&reference, Claims::default()).await;
    subscribe(&segment(&actor, "room"));
    let join = joined.wait().await;

    assert_eq!(join.segment_id, "room");
    assert!(
        !join.token_reference.is_empty() && !join.connection_id.is_empty() && join.timestamp > 0,
        "join {join:?}"
    );

    let connection_id = join.connection_id.clone();

    let left = await_presence(
        &watched,
        move |event| !event.joined && event.connection_id == connection_id,
        "the leave on close",
        EVENT_WAIT,
    );

    actor.close().await;
    let leave = left.wait().await;

    assert_eq!(leave.segment_id, "room");
    assert_eq!(leave.token_reference, join.token_reference);
} // end fn delivers_typed_join_and_leave_notifications_to_watchers

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_presence_snapshots_with_raw_metadata() {
    let reference = unique_channel_reference("plist");
    let first = connected(&reference, Claims::default()).await;
    let second = connected(&reference, Claims::default()).await;
    subscribe(&segment(&first, "room"));
    subscribe(&segment(&second, "room"));
    sleep(Duration::from_millis(2500)).await;

    let room = segment(&first, "room");
    let page = room.presence_list(1, 10).await.expect("the first page");

    assert_eq!(
        (
            page.segment_id.as_str(),
            page.total,
            page.per_page,
            page.current_page,
            page.from,
            page.to
        ),
        ("room", 2, 10, 1, 1, 2)
    );

    assert_eq!(page.connections.len(), 2);

    for connection in &page.connections {
        assert!(
            !connection.connection_id.is_empty() && connection.timestamp > 0,
            "connection {connection:?}"
        );
    }

    let beyond = room
        .presence_list(50, 10)
        .await
        .expect("a page past the end");

    assert_eq!(beyond.connections, []);
    assert!(beyond.from > beyond.to, "beyond {beyond:?}");
} // end fn pages_presence_snapshots_with_raw_metadata

/// Presence needs read access. The denial names the query by its request id,
/// so the caller hears it at once instead of waiting out the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_write_only_tokens_presence_query_at_once() {
    let write_only = connected(
        &unique_channel_reference("pdeny"),
        Claims::permission(false, true),
    )
    .await;
    let reported: Recorder<CelerisError> = Recorder::default();
    let sink = reported.clone();
    write_only
        .events()
        .on_error(move |error| sink.push(error.clone()));
    let started = Instant::now();

    let result = segment(&write_only, "room").presence_list(1, 10).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "rejected after {elapsed:?}"
    );

    let Err(CelerisError::Server(ServerError {
        error_type,
        sub_type,
        resource,
        ..
    })) = result
    else {
        panic!("got {result:?}");
    };

    assert_eq!(
        (error_type, sub_type.as_deref(), resource),
        (
            ServerErrorType::PermissionDeniedError,
            Some("PRES_LIST"),
            ServerErrorResource::Text("1".to_owned())
        )
    );

    sleep(Duration::from_millis(500)).await;

    assert_eq!(reported.all(), [], "also reported to on_error");
    assert_eq!(write_only.state(), ChannelState::Connected);
} // end fn rejects_a_write_only_tokens_presence_query_at_once

/// Watching presence is not membership: a cancelled presence subscription
/// stops its events while the message subscription keeps delivering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stops_presence_after_cancellation_while_the_subscription_stays() {
    let reference = unique_channel_reference("unwatch");
    let watcher = connected(&reference, Claims::default()).await;
    let room = segment(&watcher, "room");
    subscribe(&room);
    let watching = subscribe_presence(&room);
    settle().await;
    watching.cancel();
    settle().await;

    let events = record_presence(&room);
    let actor = connected(&reference, Claims::default()).await;
    subscribe(&segment(&actor, "room"));
    let still_member = await_body(&room, "still-member");
    publish_text(&actor, "room", "still-member").await;
    still_member.wait().await;
    sleep(Duration::from_millis(2500)).await;

    assert_eq!(events.all(), [], "presence after cancellation");
} // end fn stops_presence_after_cancellation_while_the_subscription_stays

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hides_a_connections_own_join_and_shows_it_to_a_sibling_of_the_same_token() {
    let reference = unique_channel_reference("presence-self");
    let first = connected(&reference, Claims::reference("alice")).await;
    let second = connected(&reference, Claims::reference("alice")).await;
    let first_room = segment(&first, "room");
    let second_room = segment(&second, "room");
    let first_saw = record_presence(&first_room);
    let second_saw = record_presence(&second_room);
    subscribe_presence(&first_room);
    subscribe_presence(&second_room);
    settle().await;

    let sibling_join = await_presence(
        &second_room,
        |event| event.joined,
        "the sibling's join",
        EVENT_WAIT,
    );
    subscribe(&first_room);
    let join = sibling_join.wait().await;
    settle().await;

    assert_eq!(join.token_reference, "alice");
    assert_eq!(
        first_saw.all(),
        [],
        "the joining connection saw its own join"
    );
    assert_eq!(second_saw.all(), [join]);
} // end fn hides_a_connections_own_join_and_shows_it_to_a_sibling_of_the_same_token

/// One user with three tabs: each connection of one token reference is listed
/// with its own connection id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lists_each_connection_of_one_token_reference_with_its_own_connection_id() {
    let reference = unique_channel_reference("presence-same-reference");
    let watcher = connected(&reference, Claims::reference("watcher")).await;
    let room = segment(&watcher, "room");
    let joins = record_presence(&room);
    subscribe_presence(&room);
    settle().await;

    let mut tabs: Vec<Opened> = Vec::new();

    for _ in 0..3 {
        tabs.push(connected(&reference, Claims::reference("user_1")).await);
    }

    for tab in &tabs {
        subscribe(&segment(tab, "room"));
    }

    eventually(
        || joins.count() >= 3,
        "three joins",
        Duration::from_secs(20),
    )
    .await;
    let mut join_ids = Vec::new();

    for event in joins.all() {
        assert!(
            event.joined && event.token_reference == "user_1",
            "event {event:?}, want a join of user_1"
        );

        join_ids.push(event.connection_id);
    }

    let distinct: HashSet<&String> = join_ids.iter().collect();

    assert_eq!(distinct.len(), 3, "connection ids {join_ids:?}");

    // The largest page size, 100, lists all three.
    assert_listed(&room, &join_ids).await;

    let left = await_presence(
        &room,
        |event| !event.joined,
        "the leave of one tab",
        Duration::from_secs(20),
    );
    tabs[0].close().await;
    let leave = left.wait().await;

    assert_eq!(leave.token_reference, "user_1");
    assert!(join_ids.contains(&leave.connection_id), "leave {leave:?}");

    settle().await;
    join_ids.retain(|connection_id| *connection_id != leave.connection_id);
    assert_listed(&room, &join_ids).await;
} // end fn lists_each_connection_of_one_token_reference_with_its_own_connection_id

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sends_a_leave_when_a_member_unsubscribes_and_stays_connected() {
    let reference = unique_channel_reference("presence-unsubscribe");
    let watcher = connected(&reference, Claims::default()).await;
    let actor = connected(&reference, Claims::reference("actor")).await;
    let room = segment(&watcher, "room");
    subscribe_presence(&room);
    settle().await;

    let joined = await_presence(
        &room,
        |event| event.joined && event.token_reference == "actor",
        "the actor's join",
        EVENT_WAIT,
    );

    let membership = subscribe(&segment(&actor, "room"));
    let join = joined.wait().await;

    let left = await_presence(
        &room,
        |event| !event.joined && event.token_reference == "actor",
        "the actor's leave",
        EVENT_WAIT,
    );

    membership.cancel();
    let leave = left.wait().await;

    assert_eq!(leave.connection_id, join.connection_id);
    assert_eq!(actor.state(), ChannelState::Connected);
} // end fn sends_a_leave_when_a_member_unsubscribes_and_stays_connected

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announces_default_segment_joins_on_connect_and_leaves_on_close_and_lists_every_connection()
{
    let reference = unique_channel_reference("presence-default");
    let watcher = connected(&reference, Claims::reference("watcher")).await;
    let lobby = watcher.default_segment();
    subscribe_presence(&lobby);
    settle().await;

    let joined = await_presence(
        &lobby,
        |event| event.joined && event.token_reference == "late",
        "the join from connect",
        EVENT_WAIT,
    );

    let late = connected(&reference, Claims::reference("late")).await;
    let join = joined.wait().await;

    assert_eq!(join.segment_id, "default");

    let page = lobby.presence_list(1, 10).await.expect("the presence list");
    let mut token_references: Vec<String> = page
        .connections
        .into_iter()
        .map(|connection| connection.token_reference)
        .collect();

    token_references.sort();

    assert_eq!(token_references, ["late", "watcher"]);

    let connection_id = join.connection_id.clone();

    let left = await_presence(
        &lobby,
        move |event| !event.joined && event.connection_id == connection_id,
        "the leave from close",
        EVENT_WAIT,
    );

    late.close().await;
    left.wait().await;
} // end fn announces_default_segment_joins_on_connect_and_leaves_on_close_and_lists_every_connection

/// Edge: three full pages of one entry, then page four is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_through_several_full_pages_and_past_the_end() {
    let reference = unique_channel_reference("presence-pages");
    let mut members = Vec::new();

    for token_reference in ["m1", "m2", "m3"] {
        members.push(connected(&reference, Claims::reference(token_reference)).await);
    }

    for member in &members {
        subscribe(&segment(member, "room"));
    }

    sleep(Duration::from_secs(3)).await;
    let room = segment(&members[0], "room");
    let mut pages: Vec<PresencePage> = Vec::new();

    for page in 1..=4 {
        pages.push(room.presence_list(page, 1).await.expect("a presence page"));
    }

    let mut connection_ids = HashSet::new();
    let mut token_references = Vec::new();

    for (number, page) in (1..).zip(&pages[..3]) {
        assert_eq!(
            (
                page.total,
                page.per_page,
                page.current_page,
                page.from,
                page.to,
                page.connections.len()
            ),
            (3, 1, number, number, number, 1),
            "page {number}: {page:?}"
        );

        connection_ids.insert(page.connections[0].connection_id.clone());
        token_references.push(page.connections[0].token_reference.clone());
    }

    let beyond = &pages[3];

    assert_eq!(
        (
            beyond.total,
            beyond.from,
            beyond.to,
            beyond.connections.len()
        ),
        (3, 4, 3, 0),
        "page 4: {beyond:?}"
    );

    token_references.sort();

    assert_eq!(connection_ids.len(), 3, "connection ids {connection_ids:?}");
    assert_eq!(token_references, ["m1", "m2", "m3"]);
} // end fn pages_through_several_full_pages_and_past_the_end
