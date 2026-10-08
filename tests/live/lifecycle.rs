//! Lifecycle, timeouts and the retry budget (catalogue sections 1, 3 and 9).

use std::time::Duration;

use tokio::time::{Instant, sleep, timeout};
use useceleris_client::{
    CelerisError, ChannelState, ClientOptions, CredentialReason, CredentialRequest,
};

use crate::support::channels::{
    Opened, connect, connected, open, options, publish_and_await, recording_provider, segment,
    signing_provider, subscribe, unique_channel_reference,
};
use crate::support::collect::{Recorder, assert_payloads, collect, reconnect_requests};
use crate::support::environment::websocket_url;
use crate::support::proxy::DroppingProxy;
use crate::support::signer::Claims;
use crate::support::wait::{await_recovery, await_state, eventually, settle, until_state};

/// A channel behind its own dropping proxy that records its credential
/// requests, state changes and reported errors. The channel closes before the
/// proxy stops.
struct ProxiedChannel {
    channel: Opened,
    proxy: DroppingProxy,
    requests: Recorder<CredentialRequest>,
    states: Recorder<ChannelState>,
    errors: Recorder<CelerisError>,
} // end struct ProxiedChannel

impl ProxiedChannel {
    /// Leaves the channel unconnected. `configure` sets any client option
    /// except the provider and the base URL.
    async fn start(
        label: &str,
        configure: impl FnOnce(ClientOptions) -> ClientOptions,
    ) -> ProxiedChannel {
        let proxy = DroppingProxy::start().await;
        let (provider, requests) = recording_provider(|_| Claims::default());
        let channel = open(
            configure(options(proxy.url(), provider)),
            &unique_channel_reference(label),
        );

        let states = Recorder::default();
        let state_sink = states.clone();
        channel
            .events()
            .on_state_change(move |state| state_sink.push(state));
        let errors = Recorder::default();
        let error_sink = errors.clone();
        channel
            .events()
            .on_error(move |error| error_sink.push(error.clone()));

        ProxiedChannel {
            channel,
            proxy,
            requests,
            states,
            errors,
        }
    } // end fn start

    /// Cuts every connection with RST and refuses new ones.
    fn start_outage(&self) {
        self.proxy.set_refusing(true);
        self.proxy.drop_all();
    } // end method start_outage

    /// The states seen so far; the last two once they arrive.
    fn last_states(&self) -> Vec<ChannelState> {
        let states = self.states.all();

        states[states.len().saturating_sub(2)..].to_vec()
    } // end method last_states

    /// Waits for the one error report, then checks that it is the only one
    /// and a Transport error.
    async fn assert_one_transport_error(&self) {
        eventually(
            || self.errors.count() >= 1,
            "the error report",
            Duration::from_secs(5),
        )
        .await;
        sleep(Duration::from_millis(500)).await;
        let reported = self.errors.all();

        assert!(
            reported.len() == 1 && matches!(reported[0], CelerisError::Transport(_)),
            "reported {reported:?}, want one Transport error"
        );
    } // end method assert_one_transport_error
} // end impl ProxiedChannel

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_a_connect_at_its_connect_timeout_when_the_server_never_answers() {
    let setup = ProxiedChannel::start("lifecycle-timeout", |options| ClientOptions {
        connect_timeout: Some(Duration::from_secs(2)),
        ..options
    })
    .await;

    setup.proxy.set_blackhole(true);
    let started = Instant::now();
    let result = setup.channel.connect().await;
    let elapsed = started.elapsed();

    assert_eq!(
        result,
        Err(CelerisError::Timeout(
            "Connection attempt timed out after 2000 ms.".to_owned()
        ))
    );

    assert!(
        elapsed >= Duration::from_millis(1900) && elapsed < Duration::from_secs(4),
        "failed after {elapsed:?}"
    );

    assert_eq!(setup.channel.state(), ChannelState::Failed);
    eventually(
        || setup.states.count() >= 2,
        "two state changes",
        Duration::from_secs(5),
    )
    .await;
    sleep(Duration::from_millis(500)).await;

    assert_eq!(
        setup.states.all(),
        [ChannelState::Connecting, ChannelState::Failed]
    );

    // One failure, one report: the caller only (LIFE-02).
    assert_eq!(setup.errors.all(), []);
} // end fn fails_a_connect_at_its_connect_timeout_when_the_server_never_answers

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn times_out_at_a_1ms_connect_timeout_against_the_real_server() {
    let channel = open(
        ClientOptions {
            connect_timeout: Some(Duration::from_millis(1)),
            ..options(websocket_url(), signing_provider(Claims::default()))
        },
        &unique_channel_reference("lifecycle-1ms"),
    );

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Timeout(
            "Connection attempt timed out after 1 ms.".to_owned()
        ))
    );

    assert_eq!(channel.state(), ChannelState::Failed);
} // end fn times_out_at_a_1ms_connect_timeout_against_the_real_server

/// Cancellation is dropping the future (LANG-02).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancels_a_connect_by_dropping_its_future() {
    let setup = ProxiedChannel::start("lifecycle-abort", |options| options).await;
    setup.proxy.set_blackhole(true);

    let outcome = timeout(Duration::from_millis(500), setup.channel.connect()).await;

    assert!(outcome.is_err(), "the connect settled: {outcome:?}");
    assert_eq!(setup.channel.state(), ChannelState::Failed);
    assert_eq!(setup.requests.count(), 1);
} // end fn cancels_a_connect_by_dropping_its_future

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closes_a_channel_that_is_still_connecting() {
    let setup = ProxiedChannel::start("lifecycle-close-connecting", |options| options).await;
    setup.proxy.set_blackhole(true);
    let channel = setup.channel.clone();
    let connecting = tokio::spawn(async move { channel.connect().await });
    sleep(Duration::from_millis(500)).await;

    setup.channel.close().await;
    let result = connecting.await.expect("the connect task finishes");

    assert!(
        matches!(result, Err(CelerisError::Cancelled(_))),
        "got {result:?}"
    );

    assert_eq!(setup.channel.state(), ChannelState::Closed);

    let again = setup.channel.connect().await;

    assert!(
        matches!(again, Err(CelerisError::NotConnected(_))),
        "connect after close: {again:?}"
    );
} // end fn closes_a_channel_that_is_still_connecting

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closes_a_channel_that_is_reconnecting_and_stops_its_retries() {
    let setup = ProxiedChannel::start("lifecycle-close-reconnecting", |options| options).await;
    connect(&setup.channel).await;
    setup.start_outage();
    until_state(
        &setup.channel,
        ChannelState::Reconnecting,
        Duration::from_secs(5),
    )
    .await;
    sleep(Duration::from_secs(1)).await;

    setup.channel.close().await;
    let requests_at_close = setup.requests.count();
    setup.proxy.set_refusing(false);
    sleep(Duration::from_secs(5)).await;

    assert_eq!(setup.channel.state(), ChannelState::Closed);
    assert_eq!(setup.requests.count(), requests_at_close);
} // end fn closes_a_channel_that_is_reconnecting_and_stops_its_retries

/// QUEUE-01: a publish while reconnecting waits for the reconnect. When
/// recovery stops in failed, it fails with the error that `on_error`
/// reports, and a later publish is refused at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_publish_queued_while_reconnecting_with_the_terminal_error_and_refuses_one_after_failed()
 {
    let setup = ProxiedChannel::start("lifecycle-publish-failed", |options| ClientOptions {
        maximum_reconnect_attempts: Some(2),
        ..options
    })
    .await;

    connect(&setup.channel).await;
    setup.start_outage();
    until_state(
        &setup.channel,
        ChannelState::Reconnecting,
        Duration::from_secs(5),
    )
    .await;

    let chat = segment(&setup.channel, "chat");
    let queued_chat = chat.clone();
    let queued = tokio::spawn(async move { queued_chat.publish(b"x").await });

    until_state(
        &setup.channel,
        ChannelState::Failed,
        Duration::from_secs(60),
    )
    .await;
    setup.assert_one_transport_error().await;

    let queued = queued.await.expect("the publish task finishes");

    assert_eq!(queued, Err(setup.errors.all()[0].clone()));

    assert_eq!(
        chat.publish(b"y").await,
        Err(CelerisError::NotConnected(
            "Channel is not connected; it is failed.".to_owned()
        ))
    );
} // end fn rejects_a_publish_queued_while_reconnecting_with_the_terminal_error_and_refuses_one_after_failed

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarts_from_failed_with_an_explicit_connect_and_sends_held_subscriptions() {
    let setup = ProxiedChannel::start("lifecycle-restart", |options| options).await;
    let chat = segment(&setup.channel, "chat");
    let received = collect(&chat);
    subscribe(&chat);
    setup.proxy.set_refusing(true);

    let refused = setup.channel.connect().await;

    assert!(
        matches!(refused, Err(CelerisError::Transport(_))),
        "got {refused:?}"
    );

    assert_eq!(setup.channel.state(), ChannelState::Failed);

    setup.proxy.set_refusing(false);
    connect(&setup.channel).await;
    let reference = setup.channel.reference().to_owned();
    let publisher = connected(&reference, Claims::default()).await;
    settle().await;
    publish_and_await(&publisher, &setup.channel, "chat", "after-restart").await;

    let initial = CredentialRequest {
        channel_reference: reference,
        reason: CredentialReason::Initial,
    };

    assert_eq!(setup.requests.all(), [initial.clone(), initial]);
    assert_payloads(&received, &["after-restart"]);
} // end fn restarts_from_failed_with_an_explicit_connect_and_sends_held_subscriptions

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_after_ten_failed_reconnect_attempts_and_reports_it() {
    let setup = ProxiedChannel::start("lifecycle-exhaustion", |options| options).await;
    connect(&setup.channel).await;
    let failed = await_state(
        &setup.channel,
        ChannelState::Failed,
        Duration::from_secs(200),
    );
    setup.start_outage();
    failed.wait().await;

    assert_eq!(reconnect_requests(&setup.requests).len(), 10);
    setup.assert_one_transport_error().await;
    assert_eq!(
        setup.last_states(),
        [ChannelState::Reconnecting, ChannelState::Failed]
    );
} // end fn fails_after_ten_failed_reconnect_attempts_and_reports_it

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fails_after_the_configured_maximum_of_2_reconnect_attempts() {
    let setup = ProxiedChannel::start("lifecycle-maximum", |options| ClientOptions {
        maximum_reconnect_attempts: Some(2),
        ..options
    })
    .await;

    connect(&setup.channel).await;
    let failed = await_state(
        &setup.channel,
        ChannelState::Failed,
        Duration::from_secs(60),
    );
    setup.start_outage();
    failed.wait().await;

    assert_eq!(reconnect_requests(&setup.requests).len(), 2);
    setup.assert_one_transport_error().await;
    assert_eq!(
        setup.last_states(),
        [ChannelState::Reconnecting, ChannelState::Failed]
    );
} // end fn fails_after_the_configured_maximum_of_2_reconnect_attempts

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resets_the_retry_budget_after_sixty_seconds_connected() {
    let setup = ProxiedChannel::start("lifecycle-budget", |options| options).await;
    connect(&setup.channel).await;

    // A first outage uses at least two failed attempts before it recovers:
    // once a fourth request is made, the second and third have failed.
    setup.start_outage();

    eventually(
        || setup.requests.count() >= 4,
        "two failed reconnect attempts",
        Duration::from_secs(20),
    )
    .await;

    let first_recovery = await_recovery(&setup.channel, Duration::from_secs(30));
    setup.proxy.set_refusing(false);
    let first = first_recovery.wait().await;

    assert!(
        first.retry_index >= 2,
        "first recovery {first:?}, want a retry index of at least 2"
    );

    // After sixty seconds connected, the next outage starts a new budget.
    sleep(Duration::from_secs(61)).await;
    let second_recovery = await_recovery(&setup.channel, Duration::from_secs(30));
    setup.proxy.drop_all();
    let second = second_recovery.wait().await;

    assert_eq!(second.retry_index, 0, "second recovery {second:?}");
} // end fn resets_the_retry_budget_after_sixty_seconds_connected
