//! Reconnection and recovery (catalogue sections 4, 7 and 9).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;
use useceleris_client::{ChannelState, CredentialReason, CredentialRequest, RecoveryEvent};

use crate::support::channels::{
    Opened, confirm_quiet, connect, connected, open, options, publish_all, publish_and_await,
    publish_text, recording_provider, segment, subscribe, subscribe_presence,
    unique_channel_reference,
};
use crate::support::collect::{
    Recorder, assert_payloads, collect, collect_channel, distinct_message_ids, reconnect_requests,
    tagged,
};
use crate::support::proxy::DroppingProxy;
use crate::support::signer::{Claims, Replay};
use crate::support::wait::{await_body, await_presence, eventually, settle};

/// A receiver behind the dropping proxy, and a publisher that connects
/// directly, so only the receiver has the outage. The channels close before
/// the proxy stops.
struct ReconnectSetup {
    receiver: Opened,
    publisher: Opened,
    proxy: DroppingProxy,
    requests: Recorder<CredentialRequest>,
    recoveries: Recorder<RecoveryEvent>,
} // end struct ReconnectSetup

impl ReconnectSetup {
    /// Leaves the receiver unconnected. With `replay_on_reconnect`, the
    /// provider signs the lookback that the SDK asks for into the token: the
    /// canonical mapping.
    async fn start(label: &str, replay_on_reconnect: bool) -> ReconnectSetup {
        let proxy = DroppingProxy::start().await;
        let reference = unique_channel_reference(label);

        let (provider, requests) = recording_provider(move |request| match request.reason {
            CredentialReason::Reconnect {
                replay_lookback, ..
            } if replay_on_reconnect => {
                let milliseconds = u64::try_from(replay_lookback.as_millis()).unwrap_or(u64::MAX);

                Claims::replay(Replay::Lookback(milliseconds))
            }
            _ => Claims::default(),
        });

        let receiver = open(options(proxy.url(), provider), &reference);
        let recoveries = Recorder::default();
        let sink = recoveries.clone();
        receiver
            .events()
            .on_recovery(move |event| sink.push(event.clone()));
        let publisher = connected(&reference, Claims::default()).await;

        ReconnectSetup {
            receiver,
            publisher,
            proxy,
            requests,
            recoveries,
        }
    } // end fn start

    async fn connect(&self) {
        connect(&self.receiver).await;
    } // end method connect

    /// Closes every connection through the proxy (FIN) and refuses new ones,
    /// so a message published now can reach the receiver only by replay.
    async fn start_outage(&self) {
        self.proxy.set_refusing(true);
        self.proxy.close_all();
        sleep(Duration::from_millis(500)).await;

        assert_eq!(
            self.receiver.state(),
            ChannelState::Reconnecting,
            "the state during the outage"
        );
    } // end method start_outage

    /// Stops refusing connections and waits for the recovery event.
    async fn end_outage(&self) {
        self.proxy.set_refusing(false);

        eventually(
            || self.recoveries.count() >= 1,
            "the recovery event",
            Duration::from_secs(45),
        )
        .await;
    } // end method end_outage

    fn first_recovery(&self) -> RecoveryEvent {
        self.recoveries.all()[0].clone()
    } // end method first_recovery
} // end impl ReconnectSetup

/// SUB-01, REC-02: an outage recovers with fresh credentials, a replay of
/// what it missed, and the subscription restored. The deduplication window
/// drops replayed duplicates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_after_an_outage_with_replay_and_restored_subscriptions() {
    let setup = ReconnectSetup::start("reconnect", true).await;
    let chat = segment(&setup.receiver, "chat");
    let delivered = collect(&chat);
    subscribe(&chat);
    setup.connect().await;
    settle().await;
    publish_and_await(&setup.publisher, &setup.receiver, "chat", "before").await;

    setup.start_outage().await;
    publish_text(&setup.publisher, "chat", "during").await;
    sleep(Duration::from_secs(2)).await;
    let during = await_body(&chat, "during");
    setup.end_outage().await;
    during.wait().await;

    let recovery = setup.first_recovery();

    assert!(
        recovery.possible_gaps && recovery.possible_duplicates,
        "recovery {recovery:?}"
    );

    let requests = setup.requests.all();

    for request in &requests[1..] {
        let CredentialReason::Reconnect {
            disconnected_at,
            replay_lookback,
        } = request.reason
        else {
            panic!("request {request:?}, want a reconnect");
        };

        assert!(
            disconnected_at > UNIX_EPOCH && disconnected_at <= SystemTime::now(),
            "request {request:?}"
        );

        assert!(
            replay_lookback >= Duration::from_secs(5),
            "request {request:?}"
        );
    }

    // The subscription was restored on the new connection.
    publish_and_await(&setup.publisher, &setup.receiver, "chat", "after").await;
    settle().await;

    // Replay sent "before" again, and the deduplication window dropped it.
    assert_payloads(&delivered, &["before", "during", "after"]);
    assert_eq!(distinct_message_ids(&delivered), 3);
} // end fn recovers_after_an_outage_with_replay_and_restored_subscriptions

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_every_missed_message_on_several_segments_in_order_and_one_time() {
    let setup = ReconnectSetup::start("reconnect-segments", true).await;
    let alpha_segment = segment(&setup.receiver, "alpha");
    let beta_segment = segment(&setup.receiver, "beta");
    let default_segment = setup.receiver.default_segment();
    let alpha = collect(&alpha_segment);
    let beta = collect(&beta_segment);
    let lobby = collect(&default_segment);
    let (channel_wide, _registration) = collect_channel(&setup.receiver);
    subscribe(&alpha_segment);
    subscribe(&beta_segment);
    setup.connect().await;
    settle().await;
    publish_and_await(&setup.publisher, &setup.receiver, "alpha", "a0").await;

    setup.start_outage().await;
    publish_all(&setup.publisher, "alpha", &["a1", "a2", "a3"]).await;
    publish_all(&setup.publisher, "beta", &["b1", "b2"]).await;
    publish_text(&setup.publisher, "default", "d1").await;
    sleep(Duration::from_secs(2)).await;
    let last_alpha = await_body(&alpha_segment, "a3");
    let last_beta = await_body(&beta_segment, "b2");
    let last_lobby = await_body(&default_segment, "d1");
    setup.end_outage().await;
    last_alpha.wait().await;
    last_beta.wait().await;
    last_lobby.wait().await;
    settle().await;

    assert_payloads(&alpha, &["a0", "a1", "a2", "a3"]);
    assert_payloads(&beta, &["b1", "b2"]);
    assert_payloads(&lobby, &["d1"]);

    let mut entries = tagged(&channel_wide);
    entries.sort();

    assert_eq!(
        entries,
        [
            "alpha:a0",
            "alpha:a1",
            "alpha:a2",
            "alpha:a3",
            "beta:b1",
            "beta:b2",
            "default:d1"
        ]
    );
} // end fn recovers_every_missed_message_on_several_segments_in_order_and_one_time

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loses_missed_messages_without_a_replay_claim_but_restores_the_subscription() {
    let setup = ReconnectSetup::start("reconnect-no-replay", false).await;
    let chat = segment(&setup.receiver, "chat");
    let delivered = collect(&chat);
    subscribe(&chat);
    setup.connect().await;
    settle().await;
    publish_and_await(&setup.publisher, &setup.receiver, "chat", "before").await;

    setup.start_outage().await;
    publish_text(&setup.publisher, "chat", "missed").await;
    sleep(Duration::from_secs(2)).await;
    setup.end_outage().await;
    settle().await;

    publish_and_await(&setup.publisher, &setup.receiver, "chat", "after").await;
    settle().await;

    // The recovery event declares the gap that this test makes.
    assert!(setup.first_recovery().possible_gaps);
    assert_payloads(&delivered, &["before", "after"]);
} // end fn loses_missed_messages_without_a_replay_claim_but_restores_the_subscription

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_every_missed_message_after_a_longer_outage_with_failed_attempts() {
    let setup = ReconnectSetup::start("reconnect-long", true).await;
    let chat = segment(&setup.receiver, "chat");
    let delivered = collect(&chat);
    subscribe(&chat);
    setup.connect().await;
    settle().await;

    setup.start_outage().await;
    publish_text(&setup.publisher, "chat", "m1").await;
    sleep(Duration::from_secs(3)).await;
    publish_text(&setup.publisher, "chat", "m2").await;
    sleep(Duration::from_secs(3)).await;
    publish_text(&setup.publisher, "chat", "m3").await;
    sleep(Duration::from_millis(500)).await;
    let last = await_body(&chat, "m3");
    setup.end_outage().await;
    last.wait().await;
    settle().await;

    // Retries in the first 6.5 s fail (their delays are at most 0.5, 1 and
    // 2 s), each with a fresh credential request and a longer lookback.
    let reconnects = reconnect_requests(&setup.requests);

    assert!(
        reconnects.len() >= 4,
        "{} reconnect credential requests, want at least 4",
        reconnects.len()
    );

    let outages: Vec<(SystemTime, Duration)> = reconnects
        .iter()
        .map(|request| match request.reason {
            CredentialReason::Reconnect {
                disconnected_at,
                replay_lookback,
            } => (disconnected_at, replay_lookback),
            CredentialReason::Initial => panic!("an initial request among the reconnects"),
        })
        .collect();

    let lookbacks: Vec<Duration> = outages.iter().map(|(_, lookback)| *lookback).collect();

    assert!(
        outages
            .iter()
            .all(|(disconnected_at, _)| *disconnected_at == outages[0].0),
        "the reconnect requests disagree on the disconnection time: {reconnects:?}"
    );

    assert!(
        lookbacks.is_sorted() && lookbacks[lookbacks.len() - 1] >= Duration::from_secs(11),
        "lookbacks {lookbacks:?}"
    );

    assert_payloads(&delivered, &["m1", "m2", "m3"]);
} // end fn recovers_every_missed_message_after_a_longer_outage_with_failed_attempts

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn does_not_rejoin_a_segment_that_the_connection_joined_only_by_publishing() {
    let setup = ReconnectSetup::start("reconnect-publish-join", true).await;
    let team = collect(&segment(&setup.receiver, "team"));
    setup.connect().await;
    publish_text(&setup.receiver, "team", "joining").await;
    settle().await;
    publish_and_await(&setup.publisher, &setup.receiver, "team", "before").await;

    setup.start_outage().await;
    setup.end_outage().await;
    settle().await;

    publish_text(&setup.publisher, "team", "after").await;
    confirm_quiet(&setup.publisher, &setup.receiver).await;

    assert_payloads(&team, &["before"]);
} // end fn does_not_rejoin_a_segment_that_the_connection_joined_only_by_publishing

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announces_the_new_connection_and_restores_its_presence_subscription_after_a_reconnect() {
    let setup = ReconnectSetup::start("reconnect-presence", true).await;
    let room = segment(&setup.receiver, "room");
    let watched = segment(&setup.publisher, "room");
    subscribe(&room);
    subscribe_presence(&room);
    subscribe_presence(&watched);
    let first_join = await_presence(
        &watched,
        |event| event.joined,
        "the first join",
        Duration::from_secs(20),
    );
    setup.connect().await;
    let before = first_join.wait().await;
    let old_connection = before.connection_id.clone();

    let left = await_presence(
        &watched,
        move |event| !event.joined && event.connection_id == old_connection,
        "the leave of the old connection",
        Duration::from_secs(20),
    );

    setup.start_outage().await;
    left.wait().await;
    let old_connection = before.connection_id.clone();

    let rejoined = await_presence(
        &watched,
        move |event| event.joined && event.connection_id != old_connection,
        "the join of the new connection",
        Duration::from_secs(45),
    );

    setup.end_outage().await;
    rejoined.wait().await;

    // The receiver's presence subscription came back with the reconnect.
    let actor_join = await_presence(
        &room,
        |event| event.joined && event.token_reference == "actor",
        "the actor's join at the restored watcher",
        Duration::from_secs(20),
    );

    let actor = connected(setup.receiver.reference(), Claims::reference("actor")).await;
    subscribe(&segment(&actor, "room"));
    actor_join.wait().await;
} // end fn announces_the_new_connection_and_restores_its_presence_subscription_after_a_reconnect

/// QUEUE-01: publishes made while the channel reconnects wait for the new
/// connection and go out in call order. Here the channel behind the proxy
/// publishes, and the direct one receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_publishes_made_during_an_outage_after_the_reconnect_in_call_order() {
    let setup = ReconnectSetup::start("reconnect-queued", false).await;
    let received = collect(&segment(&setup.publisher, "queue"));
    subscribe(&segment(&setup.publisher, "queue"));
    setup.connect().await;
    settle().await;
    setup.start_outage().await;

    let queue = segment(&setup.receiver, "queue");
    let mut results = Vec::new();

    for body in ["q1", "q2", "q3"] {
        let queue = queue.clone();
        results.push(tokio::spawn(
            async move { queue.publish(body.as_bytes()).await },
        ));

        // Each publish is queued before the next call starts, so the calls
        // keep their order.
        sleep(Duration::from_millis(100)).await;
    }

    setup.end_outage().await;

    for (index, result) in results.into_iter().enumerate() {
        let outcome = tokio::time::timeout(Duration::from_secs(20), result)
            .await
            .unwrap_or_else(|_| panic!("publish {} still waits after the reconnect", index + 1))
            .expect("the publish task finishes");

        assert_eq!(outcome, Ok(()), "publish {}", index + 1);
    }

    eventually(
        || received.count() >= 3,
        "the three queued publishes",
        Duration::from_secs(20),
    )
    .await;

    confirm_quiet(&setup.receiver, &setup.publisher).await;
    assert_payloads(&received, &["q1", "q2", "q3"]);
} // end fn delivers_publishes_made_during_an_outage_after_the_reconnect_in_call_order

/// QUEUE-01, SEG-01: the new connection is a member of exactly the segments
/// that the old one held, and its presence subscription comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keeps_the_same_segments_after_a_reconnect() {
    let setup = ReconnectSetup::start("reconnect-same-segments", false).await;
    let observer = &setup.publisher;
    let alpha = segment(&setup.receiver, "alpha");
    let alpha_messages = collect(&alpha);
    let beta_messages = collect(&segment(&setup.receiver, "beta"));
    let gamma_messages = collect(&segment(&setup.receiver, "gamma"));
    subscribe(&alpha);
    subscribe(&segment(&setup.receiver, "beta"));
    subscribe_presence(&alpha);
    let gamma = subscribe(&segment(&setup.receiver, "gamma"));

    let observed_alpha = segment(observer, "alpha");
    subscribe_presence(&observed_alpha);
    let first_join = await_presence(
        &observed_alpha,
        |event| event.joined,
        "the receiver's join",
        Duration::from_secs(20),
    );
    setup.connect().await;
    let before = first_join.wait().await;
    gamma.cancel();
    settle().await;

    let rejoined = await_presence(
        &observed_alpha,
        move |event| event.joined && event.connection_id != before.connection_id,
        "the join of the new connection",
        Duration::from_secs(45),
    );

    setup.start_outage().await;
    setup.end_outage().await;
    let after = rejoined.wait().await;
    settle().await;

    // The new connection is listed in alpha and beta, and not in gamma.
    for segment_id in ["alpha", "beta", "gamma"] {
        let page = segment(observer, segment_id)
            .presence_list(1, 100)
            .await
            .expect("the presence list");

        let listed = page
            .connections
            .iter()
            .any(|connection| connection.connection_id == after.connection_id);

        assert_eq!(
            listed,
            segment_id != "gamma",
            "{segment_id} lists the new connection: {:?}",
            page.connections
        );
    }

    publish_text(observer, "alpha", "to-alpha").await;
    publish_text(observer, "beta", "to-beta").await;
    publish_text(observer, "gamma", "to-gamma").await;
    confirm_quiet(observer, &setup.receiver).await;
    assert_payloads(&alpha_messages, &["to-alpha"]);
    assert_payloads(&beta_messages, &["to-beta"]);
    assert_payloads(&gamma_messages, &[]);

    // The restored presence subscription on alpha sees a new actor join.
    let actor_join = await_presence(
        &alpha,
        |event| event.joined && event.token_reference == "actor",
        "the actor's join at the restored watcher",
        Duration::from_secs(20),
    );

    let actor = connected(setup.receiver.reference(), Claims::reference("actor")).await;
    subscribe(&segment(&actor, "alpha"));
    actor_join.wait().await;
} // end fn keeps_the_same_segments_after_a_reconnect
