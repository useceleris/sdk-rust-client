//! Lifecycle (LIFE-01–04): construction, the connect states, failure and
//! restart, cancellation, state listeners, and one socket per channel.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::client::Client;
use crate::credentials::{CredentialProvider, Credentials};
use crate::errors::CelerisError;
use crate::events::Registration;
use crate::tests::support::{
    DialBehaviour, Recorder, client_with_server, error_recorder, options_with_provider,
    segment_command, settle, state_recorder, test_credentials, test_options, wait_until,
};
use crate::values::ChannelState;

const HANDSHAKE_FAILED: &str = "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.";

const PANIC_REPORT: &str = "A listener callback panicked; the channel recovered and kept running.";

#[test]
fn construction_needs_no_runtime() {
    // Outside a runtime, starting a task or a timer panics.
    let (options, _log) = test_options();
    let client = Client::new(options).unwrap();
    let channel = client.channel("test-channel").unwrap();

    channel.segment("chat").unwrap();
    channel.default_segment();
    channel.events().on_state_change(|_state| {});

    assert_eq!(channel.state(), ChannelState::Idle);
} // end fn construction_needs_no_runtime

#[tokio::test(start_paused = true)]
async fn construction_and_handles_do_no_network_work() {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let other = client.channel("test-channel").unwrap();

    channel.segment("chat").unwrap().on_message(|_, _| {});
    channel.default_segment();

    sleep(Duration::from_secs(60)).await;

    assert!(!Arc::ptr_eq(&channel.shared, &other.shared));
    assert_eq!(server.urls.len(), 0);
    assert!(!server.has_pending_connection());
    assert_eq!(log.requests.len(), 0);
    assert_eq!(Handle::current().metrics().num_alive_tasks(), 0);
    assert_eq!(channel.state(), ChannelState::Idle);
} // end fn construction_and_handles_do_no_network_work

#[tokio::test(start_paused = true)]
async fn the_states_go_idle_connecting_connected() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let states = state_recorder(&channel);
    assert_eq!(channel.state(), ChannelState::Idle);

    server.set_behaviour(DialBehaviour::Hang);
    let connecting = channel.clone();
    let attempt = tokio::spawn(async move { connecting.connect().await });

    wait_until("the dial", || server.urls.len() == 1).await;
    assert_eq!(channel.state(), ChannelState::Connecting);
    channel.close().await;
    assert_eq!(
        attempt.await.unwrap(),
        Err(CelerisError::Cancelled(
            "Connection attempt cancelled: the channel was closed or a newer attempt started."
                .to_owned()
        ))
    );

    let channel = client.channel("test-channel").unwrap();
    let states_after = state_recorder(&channel);
    server.set_behaviour(DialBehaviour::Accept);
    let (connected, _peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    assert_eq!(channel.state(), ChannelState::Connected);

    settle(&channel).await;
    assert_eq!(
        states.all(),
        vec![
            ChannelState::Connecting,
            ChannelState::Closing,
            ChannelState::Closed
        ]
    );
    assert_eq!(
        states_after.all(),
        vec![ChannelState::Connecting, ChannelState::Connected]
    );
} // end fn the_states_go_idle_connecting_connected

#[tokio::test(start_paused = true)]
async fn a_concurrent_connect_is_refused_with_operation_in_progress() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();

    server.set_behaviour(DialBehaviour::Hang);
    let connecting = channel.clone();
    let attempt = tokio::spawn(async move { connecting.connect().await });
    wait_until("the dial", || server.urls.len() == 1).await;

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::OperationInProgress(
            "Connect was already called; the channel is connecting.".to_owned()
        ))
    );

    channel.close().await;
    attempt.await.unwrap().unwrap_err();

    let channel = client.channel("test-channel").unwrap();
    server.set_behaviour(DialBehaviour::Accept);
    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::OperationInProgress(
            "Connect was already called; the channel is connected.".to_owned()
        ))
    );

    server.set_behaviour(DialBehaviour::Hang);
    drop(peer);
    wait_until("reconnecting", || {
        channel.state() == ChannelState::Reconnecting
    })
    .await;

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::OperationInProgress(
            "Connect was already called; the channel is reconnecting.".to_owned()
        ))
    );

    channel.close().await;
} // end fn a_concurrent_connect_is_refused_with_operation_in_progress

#[tokio::test(start_paused = true)]
async fn a_connect_after_close_is_refused_with_not_connected() {
    let (options, _log) = test_options();
    let (client, _server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();

    channel.close().await;

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::NotConnected(
            "Channel is closed; create a new one with Client::channel.".to_owned()
        ))
    );
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn a_connect_after_close_is_refused_with_not_connected

#[tokio::test(start_paused = true)]
async fn a_failed_first_connect_rejects_the_caller_without_an_error_event() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();

    // One log for both kinds of event, so the failed state is the control: an
    // error event would be delivered before it.
    let events = Recorder::new();
    let states = events.clone();
    let errors = events.clone();
    channel
        .events()
        .on_state_change(move |state| states.push(format!("state {state}")));
    channel
        .events()
        .on_error(move |error| errors.push(format!("error {error}")));

    server.set_behaviour(DialBehaviour::Refuse);
    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Transport(HANDSHAKE_FAILED.to_owned()))
    );
    assert_eq!(channel.state(), ChannelState::Failed);

    settle(&channel).await;
    assert_eq!(events.all(), vec!["state connecting", "state failed"]);

    // An explicit connect starts over from failed.
    server.set_behaviour(DialBehaviour::Accept);
    let (connected, _peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    assert_eq!(channel.state(), ChannelState::Connected);

    channel.close().await;
} // end fn a_failed_first_connect_rejects_the_caller_without_an_error_event

#[tokio::test(start_paused = true)]
async fn a_connect_from_inside_a_failed_state_listener_works() {
    // The first request fails; every later one succeeds.
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);

    let provider = CredentialProvider::new(move |_request| {
        let first = counter.fetch_add(1, Ordering::SeqCst) == 0;

        async move {
            if first {
                return Err::<Credentials, _>("failure".into());
            }

            Ok(test_credentials())
        }
    });

    let (client, server) = client_with_server(options_with_provider(provider), 0.0);
    let channel = client.channel("test-channel").unwrap();

    type Restart = Arc<Mutex<Option<JoinHandle<Result<(), CelerisError>>>>>;
    let restart: Restart = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&restart);
    let restarting = channel.clone();

    channel.events().on_state_change(move |state| {
        let mut slot = slot.lock().unwrap();

        if state == ChannelState::Failed && slot.is_none() {
            let channel = restarting.clone();

            // Listeners run on the blocking pool, inside the runtime.
            *slot = Some(Handle::current().spawn(async move { channel.connect().await }));
        }
    });

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Transport(
            "Credential acquisition failed: the credential provider returned an error or panicked."
                .to_owned()
        ))
    );

    let _peer = server.accept().await;
    wait_until("the restart", || restart.lock().unwrap().is_some()).await;
    let handle = restart.lock().unwrap().take().unwrap();

    assert_eq!(handle.await.unwrap(), Ok(()));
    assert_eq!(channel.state(), ChannelState::Connected);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    channel.close().await;
} // end fn a_connect_from_inside_a_failed_state_listener_works

/// Sets a flag when the provider's future is dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    } // end method drop
} // end impl Drop for DropFlag

#[tokio::test(start_paused = true)]
async fn the_callers_cancellation_gives_failed() {
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);

    let provider = CredentialProvider::new(move |_request| {
        let guard = DropFlag(Arc::clone(&flag));

        async move {
            let _guard = guard;
            std::future::pending::<()>().await;

            Ok(test_credentials())
        }
    });

    let (client, server) = client_with_server(options_with_provider(provider), 0.0);
    let channel = client.channel("test-channel").unwrap();
    let states = state_recorder(&channel);
    let errors = error_recorder(&channel);

    // Dropping the connect future is the caller's cancellation.
    let outcome = tokio::time::timeout(Duration::from_secs(1), channel.connect()).await;
    assert!(outcome.is_err(), "the connect finished: {outcome:?}");
    assert_eq!(channel.state(), ChannelState::Failed);

    wait_until("the provider dropped", || dropped.load(Ordering::SeqCst)).await;
    settle(&channel).await;

    assert_eq!(
        states.all(),
        vec![ChannelState::Connecting, ChannelState::Failed]
    );
    assert_eq!(errors.all(), Vec::new());
    assert_eq!(server.urls.len(), 0);
} // end fn the_callers_cancellation_gives_failed

#[tokio::test(start_paused = true)]
async fn state_listeners_run_in_registration_order() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let order = Recorder::new();

    let first_order = order.clone();
    let first = channel
        .events()
        .on_state_change(move |_state| first_order.push("first"));

    let second_order = order.clone();
    channel
        .events()
        .on_state_change(move |_state| second_order.push("second"));

    let (connected, _peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    settle(&channel).await;

    first.remove();
    first.remove();
    channel.close().await;
    settle(&channel).await;

    assert_eq!(
        order.all(),
        vec!["first", "second", "first", "second", "second", "second"]
    );
} // end fn state_listeners_run_in_registration_order

#[tokio::test(start_paused = true)]
async fn a_panicking_listener_is_contained_and_reported_once() {
    let (options, _log) = test_options();
    let (client, _server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let events = Recorder::new();

    // An error listener that panics is not reported, since that would report
    // into the dispatch that failed.
    channel
        .events()
        .on_error(|_error| panic!("error-listener-secret"));

    let errors = events.clone();
    channel
        .events()
        .on_error(move |error| errors.push(format!("error {error:?}")));

    // Panics on the first state change only.
    let panicked = AtomicBool::new(false);
    channel.events().on_state_change(move |_state| {
        if !panicked.swap(true, Ordering::SeqCst) {
            panic!("listener-secret");
        }
    });

    let after = events.clone();
    channel
        .events()
        .on_state_change(move |state| after.push(format!("after {state}")));

    channel.close().await;
    settle(&channel).await;

    // The report comes next, ahead of the events already queued.
    assert_eq!(
        events.all(),
        vec![
            "after closing".to_owned(),
            format!("error Transport({PANIC_REPORT:?})"),
            "after closed".to_owned(),
        ]
    );
    assert_eq!(channel.state(), ChannelState::Closed);
} // end fn a_panicking_listener_is_contained_and_reported_once

#[tokio::test(start_paused = true)]
async fn a_listener_removed_during_dispatch_does_not_run() {
    let (options, _log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let order = Recorder::new();
    let second: Arc<Mutex<Option<Registration>>> = Arc::new(Mutex::new(None));

    let first_order = order.clone();
    let second_slot = Arc::clone(&second);
    channel.events().on_state_change(move |_state| {
        first_order.push("first");

        if let Some(registration) = second_slot.lock().unwrap().as_ref() {
            registration.remove();
        }
    });

    let second_order = order.clone();
    let registration = channel
        .events()
        .on_state_change(move |_state| second_order.push("second"));
    *second.lock().unwrap() = Some(registration);

    // One listener registered twice runs twice.
    let shared_order = order.clone();
    let shared = Arc::new(move |_state: ChannelState| shared_order.push("shared"));
    let shared_again = Arc::clone(&shared);
    channel.events().on_state_change(move |state| shared(state));
    let duplicate = channel
        .events()
        .on_state_change(move |state| shared_again(state));

    server.set_behaviour(DialBehaviour::Refuse);
    channel.connect().await.unwrap_err();
    settle(&channel).await;

    // Connecting, then failed.
    assert_eq!(
        order.all(),
        vec!["first", "shared", "shared", "first", "shared", "shared"]
    );

    duplicate.remove();
    channel.close().await;
    settle(&channel).await;

    // Closing, then closed.
    assert_eq!(order.all()[6..], ["first", "shared", "first", "shared"]);
} // end fn a_listener_removed_during_dispatch_does_not_run

#[tokio::test(start_paused = true)]
async fn one_channel_opens_one_socket_for_all_its_segments() {
    let (options, log) = test_options();
    let (client, server) = client_with_server(options, 0.0);
    let channel = client.channel("test-channel").unwrap();
    let alpha = channel.segment("alpha").unwrap();
    let beta = channel.segment("beta").unwrap();
    let _alpha_subscription = alpha.subscribe().unwrap();
    let _beta_subscription = beta.subscribe().unwrap();

    let (connected, mut peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    alpha.publish_with_message_id(b"a", "m-a").await.unwrap();
    beta.publish_with_message_id(b"b", "m-b").await.unwrap();

    assert_eq!(peer.next_command().await, segment_command("SUB", "alpha"));
    assert_eq!(peer.next_command().await, segment_command("SUB", "beta"));
    assert_eq!(
        peer.next_command().await,
        "@PUB\n$5\nalpha\n$3\nm-a\n$1\na\n"
    );
    assert_eq!(
        peer.next_command().await,
        "@PUB\n$4\nbeta\n$3\nm-b\n$1\nb\n"
    );
    assert_eq!(server.urls.len(), 1);
    assert!(!server.has_pending_connection());

    // A second channel, even for the same reference, opens a second socket.
    let second = client.channel("test-channel").unwrap();
    let (connected, mut second_peer) = tokio::join!(second.connect(), server.accept());
    connected.unwrap();

    second
        .default_segment()
        .publish_with_message_id(b"c", "m-c")
        .await
        .unwrap();
    assert_eq!(
        second_peer.next_command().await,
        "@PUB\n$7\ndefault\n$3\nm-c\n$1\nc\n"
    );
    assert_eq!(server.urls.len(), 2);
    assert_eq!(log.requests.len(), 2);

    channel.close().await;
    second.close().await;
} // end fn one_channel_opens_one_socket_for_all_its_segments
