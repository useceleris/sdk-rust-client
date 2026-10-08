//! CONFIG-01: every option is validated by `Client::new` and honoured through
//! it. Negative, fractional and wrongly typed values cannot be expressed with
//! the option types (`Duration`, `usize`, `u32`, `bool`, `String`), so only
//! zero and the range limits are tested here.

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};

use crate::channel::Channel;
use crate::client::{Client, ClientOptions};
use crate::constants::MAXIMUM_COMMAND_BYTES;
use crate::credentials::CredentialReason;
use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, FakeServer, Peer, ProviderLog, Recorder, client_with_server, delivery_frame,
    error_recorder, keep_reading, settle, stall_writer, state_recorder, test_options, wait_until,
};
use crate::values::ChannelState;

const MILLISECOND: Duration = Duration::from_millis(1);

const FIFTEEN_MINUTES: Duration = Duration::from_secs(15 * 60);

const URL_REFUSED: &str = "Invalid connection URL. base_url must use wss://, or ws:// for a loopback host when allow_insecure_loopback is true.";

const HANDSHAKE_FAILED: &str = "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.";

/// The waits before each reconnect attempt with the jitter fixed at half:
/// 250 ms doubling, capped at half of 30 s.
const HALF_JITTER_DELAYS_MS: [u64; 11] = [
    250, 500, 1_000, 2_000, 4_000, 8_000, 15_000, 15_000, 15_000, 15_000, 15_000,
];

fn refusal(options: ClientOptions) -> Option<CelerisError> {
    Client::new(options).err()
} // end fn refusal

fn configuration(message: &str) -> Option<CelerisError> {
    Some(CelerisError::Configuration(message.to_owned()))
} // end fn configuration

fn timeout_rule(field: &str) -> String {
    format!("Invalid client options. {field}: Must be from 1 ms to 15 minutes.")
} // end fn timeout_rule

/// Options with one timeout set, by field name.
fn with_timeout(field: &str, timeout: Duration) -> ClientOptions {
    let (mut options, _log) = test_options();

    match field {
        "connect_timeout" => options.connect_timeout = Some(timeout),
        "reconnect_timeout" => options.reconnect_timeout = Some(timeout),
        "presence_query_timeout" => options.presence_query_timeout = Some(timeout),
        _ => unreachable!("unknown timeout field {field}"),
    }

    options
} // end fn with_timeout

#[test]
fn each_timeout_refuses_zero_naming_the_field() {
    for field in [
        "connect_timeout",
        "reconnect_timeout",
        "presence_query_timeout",
    ] {
        assert_eq!(
            refusal(with_timeout(field, Duration::ZERO)),
            configuration(&timeout_rule(field)),
            "{field}"
        );
    }
} // end fn each_timeout_refuses_zero_naming_the_field

#[test]
fn each_timeout_accepts_one_millisecond_to_fifteen_minutes() {
    for field in [
        "connect_timeout",
        "reconnect_timeout",
        "presence_query_timeout",
    ] {
        assert_eq!(refusal(with_timeout(field, MILLISECOND)), None, "{field}");
        assert_eq!(
            refusal(with_timeout(field, FIFTEEN_MINUTES)),
            None,
            "{field}"
        );

        for refused in [
            FIFTEEN_MINUTES + MILLISECOND,
            FIFTEEN_MINUTES + Duration::from_nanos(1),
            MILLISECOND - Duration::from_nanos(1),
        ] {
            assert_eq!(
                refusal(with_timeout(field, refused)),
                configuration(&timeout_rule(field)),
                "{field} {refused:?}"
            );
        }
    }
} // end fn each_timeout_accepts_one_millisecond_to_fifteen_minutes

#[test]
fn each_size_refuses_zero_naming_the_field() {
    let (options, _log) = test_options();

    let queue = ClientOptions {
        publish_queue_size: Some(0),
        ..options.clone()
    };

    assert_eq!(
        refusal(queue),
        configuration("Invalid client options. publish_queue_size: Must be at least 1.")
    );

    let window = ClientOptions {
        deduplication_window_size: Some(0),
        ..options.clone()
    };

    assert_eq!(
        refusal(window),
        configuration("Invalid client options. deduplication_window_size: Must be at least 1.")
    );

    let smallest = ClientOptions {
        publish_queue_size: Some(1),
        deduplication_window_size: Some(1),
        ..options
    };

    assert_eq!(refusal(smallest), None);
} // end fn each_size_refuses_zero_naming_the_field

#[test]
fn maximum_reconnect_attempts_accepts_one_to_one_hundred() {
    let (options, _log) = test_options();
    let with_maximum = |maximum| ClientOptions {
        maximum_reconnect_attempts: Some(maximum),
        ..options.clone()
    };

    for refused in [0, 101] {
        assert_eq!(
            refusal(with_maximum(refused)),
            configuration(
                "Invalid client options. maximum_reconnect_attempts: Must be from 1 to 100."
            ),
            "{refused}"
        );
    }

    assert_eq!(refusal(with_maximum(1)), None);
    assert_eq!(refusal(with_maximum(100)), None);
} // end fn maximum_reconnect_attempts_accepts_one_to_one_hundred

#[test]
fn every_failed_field_is_named_in_one_error() {
    let (options, _log) = test_options();

    let options = ClientOptions {
        connect_timeout: Some(Duration::ZERO),
        reconnect_timeout: Some(Duration::ZERO),
        presence_query_timeout: Some(Duration::ZERO),
        publish_queue_size: Some(0),
        deduplication_window_size: Some(0),
        maximum_reconnect_attempts: Some(0),
        ..options
    };

    assert_eq!(
        refusal(options),
        configuration(
            "Invalid client options. connect_timeout: Must be from 1 ms to 15 minutes. reconnect_timeout: Must be from 1 ms to 15 minutes. presence_query_timeout: Must be from 1 ms to 15 minutes. publish_queue_size: Must be at least 1. deduplication_window_size: Must be at least 1. maximum_reconnect_attempts: Must be from 1 to 100."
        )
    );
} // end fn every_failed_field_is_named_in_one_error

#[test]
fn a_base_url_that_is_not_secure_or_loopback_is_refused() {
    let (options, _log) = test_options();
    let with_url = |base_url: &str, allow_insecure_loopback| ClientOptions {
        base_url: Some(base_url.to_owned()),
        allow_insecure_loopback,
        ..options.clone()
    };

    for base_url in [
        "https://example.test",
        "http://localhost",
        "ws://example.test",
    ] {
        assert_eq!(
            refusal(with_url(base_url, true)),
            configuration(URL_REFUSED),
            "{base_url}"
        );
    }

    assert_eq!(
        refusal(with_url("", false)),
        configuration("Invalid connection URL. base_url is not an absolute URL.")
    );
    assert_eq!(refusal(with_url("wss://example.test", false)), None);

    // ENDPOINT-01: without a base URL the client uses the production endpoint.
    let default = Client::new(ClientOptions {
        base_url: None,
        allow_insecure_loopback: false,
        ..options.clone()
    })
    .unwrap();

    assert_eq!(
        default.configuration.base_url,
        "wss://realtime.useceleris.com"
    );
} // end fn a_base_url_that_is_not_secure_or_loopback_is_refused

#[test]
fn loopback_hosts_need_the_opt_in() {
    let (options, _log) = test_options();
    let with_url = |base_url: &str, allow_insecure_loopback| ClientOptions {
        base_url: Some(base_url.to_owned()),
        allow_insecure_loopback,
        ..options.clone()
    };

    for base_url in [
        "ws://localhost:8080",
        "ws://127.0.0.1:8080",
        "ws://127.1.2.3",
        "ws://[::1]:8080",
    ] {
        assert_eq!(
            refusal(with_url(base_url, false)),
            configuration(URL_REFUSED),
            "{base_url}"
        );
        assert_eq!(refusal(with_url(base_url, true)), None, "{base_url}");
    }

    // The opt-in is false unless set.
    let absent = ClientOptions {
        base_url: Some("ws://localhost:8080".to_owned()),
        ..ClientOptions::new(options.credential_provider.clone())
    };

    assert_eq!(refusal(absent), configuration(URL_REFUSED));
} // end fn loopback_hosts_need_the_opt_in

#[tokio::test(start_paused = true)]
async fn a_loopback_url_with_the_opt_in_connects_there() {
    let (options, _log) = test_options();

    let options = ClientOptions {
        base_url: Some("ws://localhost:8080".to_owned()),
        allow_insecure_loopback: true,
        ..options
    };

    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("room-1").unwrap();
    let (connected, _peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let urls = server.urls.all();
    assert_eq!(urls.len(), 1);
    assert!(
        urls[0].starts_with("ws://localhost:8080/channel/room-1?"),
        "{}",
        urls[0]
    );

    channel.close().await;
} // end fn a_loopback_url_with_the_opt_in_connects_there

/// A test client with these option changes, its first channel and the
/// recording provider.
fn channel_with(
    configure: impl FnOnce(&mut ClientOptions),
    random: f64,
) -> (Channel, Arc<FakeServer>, ProviderLog) {
    let (mut options, log) = test_options();
    configure(&mut options);
    let (client, server) = client_with_server(options, random);
    let channel = client.channel("test-channel").unwrap();

    (channel, server, log)
} // end fn channel_with

/// The first connect attempt is pending at `timeout` − 1 ms and fails with
/// Timeout at `timeout`, leaving the channel failed.
async fn expect_connect_timeout_at(channel: &Channel, server: &FakeServer, timeout: Duration) {
    server.set_behaviour(DialBehaviour::Hang);
    let start = Instant::now();
    let connecting = channel.clone();
    let attempt = tokio::spawn(async move { connecting.connect().await });

    sleep(timeout - MILLISECOND).await;
    assert!(
        !attempt.is_finished(),
        "the attempt ended before {timeout:?}"
    );
    assert_eq!(channel.state(), ChannelState::Connecting);

    let result = attempt.await.unwrap();
    assert_eq!(start.elapsed(), timeout);
    assert_eq!(
        result,
        Err(CelerisError::Timeout(format!(
            "Connection attempt timed out after {} ms.",
            timeout.as_millis()
        )))
    );
    assert_eq!(channel.state(), ChannelState::Failed);
    server.set_behaviour(DialBehaviour::Accept);
} // end fn expect_connect_timeout_at

/// A presence query is pending at `timeout` − 1 ms and fails with Timeout at
/// `timeout`; the connection stays up. Returns the reader that keeps the
/// connection, and the frames that it receives.
async fn expect_presence_timeout_at(
    channel: &Channel,
    mut peer: Peer,
    timeout: Duration,
) -> (JoinHandle<()>, mpsc::UnboundedReceiver<String>) {
    let chat = channel.segment("chat").unwrap();
    let start = Instant::now();
    let query = tokio::spawn(async move { chat.presence_list(1, 25).await });

    let command = peer.next_command().await;
    assert!(command.starts_with("@PRES_LIST\n"), "{command:?}");

    // The peer keeps reading, so it answers the heartbeat's pings.
    let (reader, mut frames) = keep_reading(peer);

    sleep_until(start + timeout - MILLISECOND).await;
    assert!(!query.is_finished(), "the query ended before {timeout:?}");

    let result = query.await.unwrap();
    assert_eq!(start.elapsed(), timeout);
    assert_eq!(
        result,
        Err(CelerisError::Timeout(format!(
            "Presence query timed out after {} ms.",
            timeout.as_millis()
        )))
    );
    assert_eq!(channel.state(), ChannelState::Connected);

    // The same socket still carries commands.
    channel
        .default_segment()
        .publish_with_message_id(b"after", "after")
        .await
        .unwrap();
    assert_eq!(
        frames.recv().await.unwrap(),
        "@PUB\n$7\ndefault\n$5\nafter\n$5\nafter\n"
    );

    (reader, frames)
} // end fn expect_presence_timeout_at

/// Drops the connection while every dial hangs: the first reconnect attempt
/// is pending at `timeout` − 1 ms and times out at `timeout`, when the next
/// attempt (with no jitter delay) asks for credentials.
async fn expect_reconnect_timeout_at(
    channel: &Channel,
    server: &FakeServer,
    log: &ProviderLog,
    reader: JoinHandle<()>,
    timeout: Duration,
) {
    server.set_behaviour(DialBehaviour::Hang);
    let before = log.requests.len();
    reader.abort();

    wait_until("the first reconnect attempt", || {
        log.requests.len() == before + 1
    })
    .await;

    let started = log.times.all()[before];
    sleep_until(started + timeout - MILLISECOND).await;
    assert_eq!(log.requests.len(), before + 1);
    assert_eq!(channel.state(), ChannelState::Reconnecting);

    wait_until("the second reconnect attempt", || {
        log.requests.len() == before + 2
    })
    .await;

    assert_eq!(log.times.all()[before + 1] - started, timeout);
    assert_eq!(channel.state(), ChannelState::Reconnecting);
} // end fn expect_reconnect_timeout_at

/// Times the connect, presence and reconnect timeouts in turn, then checks
/// that close leaves no task behind.
async fn expect_timeouts(
    configure: impl FnOnce(&mut ClientOptions),
    connect: Duration,
    reconnect: Duration,
    presence: Duration,
) {
    let (channel, server, log) = channel_with(configure, 0.0);

    expect_connect_timeout_at(&channel, &server, connect).await;

    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let (reader, _frames) = expect_presence_timeout_at(&channel, peer, presence).await;
    expect_reconnect_timeout_at(&channel, &server, &log, reader, reconnect).await;

    channel.close().await;
    sleep(MILLISECOND).await;
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
} // end fn expect_timeouts

#[tokio::test(start_paused = true)]
async fn a_connect_timeout_of_5000_ms_fires_at_exactly_5000_ms() {
    let (channel, server, _log) = channel_with(
        |options| options.connect_timeout = Some(Duration::from_millis(5_000)),
        0.0,
    );

    expect_connect_timeout_at(&channel, &server, Duration::from_millis(5_000)).await;
} // end fn a_connect_timeout_of_5000_ms_fires_at_exactly_5000_ms

#[tokio::test(start_paused = true)]
async fn the_default_timeouts_are_15_s_15_s_and_10_s() {
    expect_timeouts(
        |_options| {},
        Duration::from_secs(15),
        Duration::from_secs(15),
        Duration::from_secs(10),
    )
    .await;
} // end fn the_default_timeouts_are_15_s_15_s_and_10_s

#[tokio::test(start_paused = true)]
async fn a_reconnect_timeout_equal_to_the_connect_timeout() {
    expect_timeouts(
        |options| {
            options.connect_timeout = Some(Duration::from_millis(5_000));
            options.reconnect_timeout = Some(Duration::from_millis(5_000));
        },
        Duration::from_millis(5_000),
        Duration::from_millis(5_000),
        Duration::from_secs(10),
    )
    .await;
} // end fn a_reconnect_timeout_equal_to_the_connect_timeout

#[tokio::test(start_paused = true)]
async fn a_reconnect_timeout_longer_than_the_connect_timeout() {
    expect_timeouts(
        |options| {
            options.connect_timeout = Some(Duration::from_millis(2_000));
            options.reconnect_timeout = Some(Duration::from_millis(7_000));
        },
        Duration::from_millis(2_000),
        Duration::from_millis(7_000),
        Duration::from_secs(10),
    )
    .await;
} // end fn a_reconnect_timeout_longer_than_the_connect_timeout

#[tokio::test(start_paused = true)]
async fn a_reconnect_timeout_shorter_than_the_connect_timeout() {
    expect_timeouts(
        |options| {
            options.connect_timeout = Some(Duration::from_millis(7_000));
            options.reconnect_timeout = Some(Duration::from_millis(2_000));
        },
        Duration::from_millis(7_000),
        Duration::from_millis(2_000),
        Duration::from_secs(10),
    )
    .await;
} // end fn a_reconnect_timeout_shorter_than_the_connect_timeout

#[tokio::test(start_paused = true)]
async fn an_absent_reconnect_timeout_follows_the_connect_timeout() {
    expect_timeouts(
        |options| options.connect_timeout = Some(Duration::from_millis(4_000)),
        Duration::from_millis(4_000),
        Duration::from_millis(4_000),
        Duration::from_secs(10),
    )
    .await;
} // end fn an_absent_reconnect_timeout_follows_the_connect_timeout

#[tokio::test(start_paused = true)]
async fn a_presence_timeout_of_2000_ms_fires_at_exactly_2000_ms() {
    expect_timeouts(
        |options| options.presence_query_timeout = Some(Duration::from_millis(2_000)),
        Duration::from_secs(15),
        Duration::from_secs(15),
        Duration::from_millis(2_000),
    )
    .await;
} // end fn a_presence_timeout_of_2000_ms_fires_at_exactly_2000_ms

#[tokio::test(start_paused = true)]
async fn all_three_timeouts_at_15_minutes_fire_at_exactly_900000_ms() {
    expect_timeouts(
        |options| {
            options.connect_timeout = Some(FIFTEEN_MINUTES);
            options.reconnect_timeout = Some(FIFTEEN_MINUTES);
            options.presence_query_timeout = Some(FIFTEEN_MINUTES);
        },
        Duration::from_millis(900_000),
        Duration::from_millis(900_000),
        Duration::from_millis(900_000),
    )
    .await;
} // end fn all_three_timeouts_at_15_minutes_fire_at_exactly_900000_ms

#[tokio::test(start_paused = true)]
async fn when_the_retries_run_out_the_error_names_the_reconnect_timeout() {
    let (channel, server, log) = channel_with(
        |options| {
            options.connect_timeout = Some(Duration::from_millis(7_000));
            options.reconnect_timeout = Some(Duration::from_millis(2_000));
        },
        0.0,
    );

    let errors = error_recorder(&channel);
    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    server.set_behaviour(DialBehaviour::Hang);
    let dropped = Instant::now();
    drop(peer);

    // Ten attempts of 2000 ms each, with no delay between them.
    sleep_until(dropped + Duration::from_millis(20_000) - MILLISECOND).await;
    assert_eq!(channel.state(), ChannelState::Reconnecting);
    assert_eq!(log.requests.len(), 11);

    // The tenth attempt started 18000 ms after the drop.
    assert_eq!(log.times.all()[10] - dropped, Duration::from_millis(18_000));

    wait_until("failed", || channel.state() == ChannelState::Failed).await;
    settle(&channel).await;

    assert_eq!(
        errors.all(),
        vec![CelerisError::Timeout(
            "Connection attempt timed out after 2000 ms.".to_owned()
        )]
    );
} // end fn when_the_retries_run_out_the_error_names_the_reconnect_timeout

#[tokio::test(start_paused = true)]
async fn a_publish_queue_of_one_behind_a_full_writer() {
    let (channel, server, _log) = channel_with(|options| options.publish_queue_size = Some(1), 0.0);
    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let stuck = stall_writer(&channel).await;
    let lobby = channel.default_segment();
    let first_lobby = lobby.clone();
    let first =
        tokio::spawn(async move { first_lobby.publish_with_message_id(b"one", "first").await });

    sleep(MILLISECOND).await;
    assert!(!first.is_finished(), "the first publish did not wait");

    assert_eq!(
        lobby.publish(b"two").await,
        Err(CelerisError::Backpressure(
            "The publish queue is full (size 1). Retry once some publishes have gone out."
                .to_owned()
        ))
    );

    // Reading drains the pipe, then the writer sends the waiting publish.
    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);
    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);
    assert_eq!(
        peer.next_command().await,
        "@PUB\n$7\ndefault\n$5\nfirst\n$3\none\n"
    );

    assert_eq!(stuck.await.unwrap(), Ok(()));
    assert_eq!(first.await.unwrap(), Ok(()));
} // end fn a_publish_queue_of_one_behind_a_full_writer

/// Queues `count` publishes behind a stalled writer, in order.
async fn queue_publishes(
    channel: &Channel,
    count: usize,
) -> Vec<JoinHandle<Result<(), CelerisError>>> {
    let mut queued = Vec::new();

    for index in 0..count {
        let lobby = channel.default_segment();

        queued.push(tokio::spawn(async move {
            lobby
                .publish_with_message_id(b"x", &format!("m-{index}"))
                .await
        }));

        // Each publish reaches the queue before the next one starts.
        tokio::task::yield_now().await;
    }

    sleep(MILLISECOND).await;
    assert!(queued.iter().all(|publish| !publish.is_finished()));

    queued
} // end fn queue_publishes

fn publish_frame(index: usize) -> String {
    let message_id = format!("m-{index}");

    format!(
        "@PUB\n$7\ndefault\n${}\n{message_id}\n$1\nx\n",
        message_id.len()
    )
} // end fn publish_frame

#[tokio::test(start_paused = true)]
async fn the_default_queue_refuses_the_65th_waiting_publish() {
    let (channel, server, _log) = channel_with(|_options| {}, 0.0);
    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let stuck = stall_writer(&channel).await;
    let queued = queue_publishes(&channel, 64).await;

    assert_eq!(
        channel.default_segment().publish(b"x").await,
        Err(CelerisError::Backpressure(
            "The publish queue is full (size 64). Retry once some publishes have gone out."
                .to_owned()
        ))
    );

    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);
    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);

    for index in 0..64 {
        assert_eq!(peer.next_command().await, publish_frame(index));
    }

    assert_eq!(stuck.await.unwrap(), Ok(()));

    for publish in queued {
        assert_eq!(publish.await.unwrap(), Ok(()));
    }
} // end fn the_default_queue_refuses_the_65th_waiting_publish

#[tokio::test(start_paused = true)]
async fn many_queued_publishes_drain_in_order() {
    let (channel, server, _log) =
        channel_with(|options| options.publish_queue_size = Some(500), 0.0);
    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let stuck = stall_writer(&channel).await;
    let queued = queue_publishes(&channel, 500).await;

    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);
    assert_eq!(peer.next_frame().await.len(), MAXIMUM_COMMAND_BYTES);

    for index in 0..500 {
        assert_eq!(peer.next_command().await, publish_frame(index));
    }

    assert_eq!(stuck.await.unwrap(), Ok(()));

    for publish in queued {
        assert_eq!(publish.await.unwrap(), Ok(()));
    }
} // end fn many_queued_publishes_drain_in_order

/// Connects and records the message id of every delivery.
async fn connect_recording(
    configure: impl FnOnce(&mut ClientOptions),
) -> (Channel, Peer, Recorder<String>) {
    let (channel, server, _log) = channel_with(configure, 0.0);
    let delivered = Recorder::new();
    let collector = delivered.clone();
    channel
        .events()
        .on_message(move |_payload, metadata| collector.push(metadata.message_id.clone()));

    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    (channel, peer, delivered)
} // end fn connect_recording

async fn receive(peer: &mut Peer, message_id: &str) {
    peer.send(delivery_frame("alice", "chat", message_id, 1, b"x"))
        .await;
} // end fn receive

#[tokio::test(start_paused = true)]
async fn a_window_of_one_drops_a_repeat_and_delivers_an_evicted_id_again() {
    let (_channel, mut peer, delivered) =
        connect_recording(|options| options.deduplication_window_size = Some(1)).await;

    for message_id in ["A", "A", "B", "A", "control"] {
        receive(&mut peer, message_id).await;
    }

    wait_until("the control message", || delivered.len() == 4).await;
    assert_eq!(delivered.all(), vec!["A", "B", "A", "control"]);
} // end fn a_window_of_one_drops_a_repeat_and_delivers_an_evicted_id_again

#[tokio::test(start_paused = true)]
async fn the_default_window_remembers_1024_ids() {
    let (_channel, mut peer, delivered) = connect_recording(|_options| {}).await;
    let mut expected = Vec::new();

    for index in 0..1024 {
        let message_id = format!("id-{index}");
        receive(&mut peer, &message_id).await;
        expected.push(message_id);
    }

    // id-0 is still remembered; id-1024 evicts it, so it is delivered again.
    for message_id in ["id-0", "id-1024", "id-0", "control"] {
        receive(&mut peer, message_id).await;
    }

    expected.extend(["id-1024", "id-0", "control"].map(str::to_owned));

    wait_until("the control message", || delivered.len() == 1027).await;
    assert_eq!(delivered.all(), expected);
} // end fn the_default_window_remembers_1024_ids

/// Connects with the jitter fixed at half, then drops the connection while
/// every dial is refused.
async fn drop_into_refused_reconnects(
    maximum: Option<u32>,
) -> (
    Channel,
    ProviderLog,
    Recorder<ChannelState>,
    Recorder<CelerisError>,
    Instant,
) {
    let (channel, server, log) =
        channel_with(|options| options.maximum_reconnect_attempts = maximum, 0.5);

    let states = state_recorder(&channel);
    let errors = error_recorder(&channel);
    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    server.set_behaviour(DialBehaviour::Refuse);
    let dropped = Instant::now();
    drop(peer);

    (channel, log, states, errors, dropped)
} // end fn drop_into_refused_reconnects

/// Checks that the next attempt asks for credentials exactly `delay` after
/// `previous`, and not one millisecond before. Returns when it asked.
async fn expect_attempt_after(
    channel: &Channel,
    log: &ProviderLog,
    previous: Instant,
    delay: Duration,
) -> Instant {
    let before = log.requests.len();

    sleep_until(previous + delay - MILLISECOND).await;
    assert_eq!(
        log.requests.len(),
        before,
        "an attempt came before {delay:?}"
    );
    assert_eq!(channel.state(), ChannelState::Reconnecting);

    wait_until("the next reconnect attempt", || {
        log.requests.len() == before + 1
    })
    .await;

    let asked = log.times.all()[before];
    assert_eq!(asked - previous, delay);

    asked
} // end fn expect_attempt_after

fn reconnect_requests(log: &ProviderLog) -> usize {
    log.requests
        .all()
        .iter()
        .filter(|request| matches!(request.reason, CredentialReason::Reconnect { .. }))
        .count()
} // end fn reconnect_requests

async fn expect_failure_after_attempts(maximum: Option<u32>, failures: usize) {
    let (channel, log, states, errors, dropped) = drop_into_refused_reconnects(maximum).await;
    let mut previous = dropped;

    for delay in &HALF_JITTER_DELAYS_MS[..failures] {
        previous =
            expect_attempt_after(&channel, &log, previous, Duration::from_millis(*delay)).await;
    }

    wait_until("failed", || channel.state() == ChannelState::Failed).await;
    settle(&channel).await;

    assert_eq!(
        errors.all(),
        vec![CelerisError::Transport(HANDSHAKE_FAILED.to_owned())]
    );
    assert_eq!(
        states.all(),
        vec![
            ChannelState::Connecting,
            ChannelState::Connected,
            ChannelState::Reconnecting,
            ChannelState::Failed,
        ]
    );

    sleep(Duration::from_secs(3600)).await;
    assert_eq!(reconnect_requests(&log), failures);
} // end fn expect_failure_after_attempts

#[tokio::test(start_paused = true)]
async fn a_maximum_of_one_fails_after_one_failed_attempt() {
    expect_failure_after_attempts(Some(1), 1).await;
} // end fn a_maximum_of_one_fails_after_one_failed_attempt

#[tokio::test(start_paused = true)]
async fn a_maximum_of_three_fails_after_three_failed_attempts() {
    expect_failure_after_attempts(Some(3), 3).await;
} // end fn a_maximum_of_three_fails_after_three_failed_attempts

#[tokio::test(start_paused = true)]
async fn the_default_maximum_fails_after_ten_failed_attempts() {
    expect_failure_after_attempts(None, 10).await;
} // end fn the_default_maximum_fails_after_ten_failed_attempts

#[tokio::test(start_paused = true)]
async fn a_maximum_of_100_still_retries_after_the_tenth_failure() {
    let (channel, log, _states, errors, dropped) = drop_into_refused_reconnects(Some(100)).await;
    let mut previous = dropped;

    for delay in HALF_JITTER_DELAYS_MS {
        previous =
            expect_attempt_after(&channel, &log, previous, Duration::from_millis(delay)).await;
    }

    settle(&channel).await;
    assert_eq!(reconnect_requests(&log), 11);
    assert_eq!(channel.state(), ChannelState::Reconnecting);
    assert_eq!(errors.all(), Vec::new());

    channel.close().await;
} // end fn a_maximum_of_100_still_retries_after_the_tenth_failure
