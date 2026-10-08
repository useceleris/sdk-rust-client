//! Credentials (AUTH-01–05, D-001, D-003): fresh credentials for every
//! attempt, fixed safe errors, and one deadline per attempt.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use tokio::time::{Instant, sleep};

use crate::channel::Channel;
use crate::client::ClientOptions;
use crate::credentials::{CredentialProvider, CredentialReason, CredentialRequest, Credentials};
use crate::errors::CelerisError;
use crate::tests::support::{
    DialBehaviour, FakeServer, client_with_server, error_recorder, options_with_provider, settle,
    state_recorder, test_credentials, test_options, wait_until,
};
use crate::values::ChannelState;

const PROVIDER_FAILED: &str =
    "Credential acquisition failed: the credential provider returned an error or panicked.";

const HANDSHAKE_FAILED: &str = "WebSocket handshake failed: the server refused the connection or could not be reached. Check the base URL, the credentials and the channel reference.";

fn channel_for(options: ClientOptions) -> (Channel, Arc<FakeServer>) {
    let (client, server) = client_with_server(options, 0.5);

    (client.channel("test-channel").unwrap(), server)
} // end fn channel_for

/// Sets a flag when the provider's future is dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    } // end method drop
} // end impl Drop for DropFlag

#[tokio::test(start_paused = true)]
async fn each_attempt_asks_for_new_credentials() {
    let (options, log) = test_options();
    let (channel, server) = channel_for(options);

    // Each explicit connect asks, with the initial reason.
    server.set_behaviour(DialBehaviour::Refuse);
    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Transport(HANDSHAKE_FAILED.to_owned()))
    );

    server.set_behaviour(DialBehaviour::Accept);
    let (connected, peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();

    let initial = CredentialRequest {
        channel_reference: "test-channel".to_owned(),
        reason: CredentialReason::Initial,
    };

    assert_eq!(log.requests.all(), vec![initial.clone(), initial.clone()]);

    // Each reconnect attempt asks again, with the same disconnection time and
    // a lookback of the outage so far plus five seconds. The jitter is fixed
    // at half, so the attempts come 250 ms and 750 ms after the drop.
    server.set_behaviour(DialBehaviour::Refuse);
    let before_drop = SystemTime::now();
    drop(peer);

    wait_until("the first reconnect attempt", || log.requests.len() == 3).await;
    let after_drop = SystemTime::now();

    let CredentialReason::Reconnect {
        disconnected_at, ..
    } = log.requests.all()[2].reason
    else {
        panic!("the third request is not a reconnect");
    };

    assert!(before_drop <= disconnected_at && disconnected_at <= after_drop);

    wait_until("the second reconnect attempt", || log.requests.len() == 4).await;
    server.set_behaviour(DialBehaviour::Accept);
    let _peer = server.accept().await;
    wait_until("connected", || channel.state() == ChannelState::Connected).await;

    let reconnect = |lookback_ms| CredentialRequest {
        channel_reference: "test-channel".to_owned(),
        reason: CredentialReason::Reconnect {
            disconnected_at,
            replay_lookback: Duration::from_millis(lookback_ms),
        },
    };

    assert_eq!(
        log.requests.all(),
        vec![
            initial.clone(),
            initial,
            reconnect(5_250),
            reconnect(5_750),
            reconnect(6_750),
        ]
    );

    channel.close().await;
} // end fn each_attempt_asks_for_new_credentials

#[tokio::test(start_paused = true)]
async fn a_provider_error_fails_the_attempt_with_a_fixed_safe_error() {
    let provider = CredentialProvider::new(|_request| async {
        Err::<Credentials, _>("synthetic-secret".into())
    });

    let (channel, server) = channel_for(options_with_provider(provider));
    let result = channel.connect().await;

    assert_eq!(
        result,
        Err(CelerisError::Transport(PROVIDER_FAILED.to_owned()))
    );
    assert_eq!(channel.state(), ChannelState::Failed);
    assert_eq!(server.urls.len(), 0);
} // end fn a_provider_error_fails_the_attempt_with_a_fixed_safe_error

#[tokio::test(start_paused = true)]
async fn a_provider_panic_fails_the_attempt_with_a_fixed_safe_error() {
    let provider = CredentialProvider::new(|_request| async {
        if true {
            panic!("synthetic-secret");
        }

        Ok(test_credentials())
    });

    let (channel, server) = channel_for(options_with_provider(provider));
    let result = channel.connect().await;

    assert_eq!(
        result,
        Err(CelerisError::Transport(PROVIDER_FAILED.to_owned()))
    );
    assert_eq!(channel.state(), ChannelState::Failed);
    assert_eq!(server.urls.len(), 0);
} // end fn a_provider_panic_fails_the_attempt_with_a_fixed_safe_error

#[tokio::test(start_paused = true)]
async fn a_provider_that_times_out_fails_the_attempt_and_its_late_credentials_open_nothing() {
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);

    let provider = CredentialProvider::new(move |_request| {
        let guard = DropFlag(Arc::clone(&flag));

        async move {
            let _guard = guard;
            sleep(Duration::from_secs(10)).await;

            Ok(test_credentials())
        }
    });

    let options = ClientOptions {
        connect_timeout: Some(Duration::from_secs(5)),
        ..options_with_provider(provider)
    };

    let (channel, server) = channel_for(options);
    let start = Instant::now();

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::Timeout(
            "Connection attempt timed out after 5000 ms.".to_owned()
        ))
    );
    assert_eq!(start.elapsed(), Duration::from_secs(5));

    // The provider's future is dropped, and nothing opens after its 10 s.
    wait_until("the provider dropped", || dropped.load(Ordering::SeqCst)).await;
    sleep(Duration::from_secs(60)).await;
    assert_eq!(server.urls.len(), 0);
    assert!(!server.has_pending_connection());
    assert_eq!(channel.state(), ChannelState::Failed);
} // end fn a_provider_that_times_out_fails_the_attempt_and_its_late_credentials_open_nothing

#[tokio::test(start_paused = true)]
async fn a_cancelled_attempt_drops_the_provider_and_its_late_credentials_open_nothing() {
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);

    let provider = CredentialProvider::new(move |_request| {
        let guard = DropFlag(Arc::clone(&flag));

        async move {
            let _guard = guard;
            sleep(Duration::from_secs(10)).await;

            Ok(test_credentials())
        }
    });

    let (channel, server) = channel_for(options_with_provider(provider));

    // The caller stops waiting after one second.
    let outcome = tokio::time::timeout(Duration::from_secs(1), channel.connect()).await;
    assert!(outcome.is_err(), "the connect finished: {outcome:?}");
    assert_eq!(channel.state(), ChannelState::Failed);

    wait_until("the provider dropped", || dropped.load(Ordering::SeqCst)).await;
    sleep(Duration::from_secs(60)).await;
    assert_eq!(server.urls.len(), 0);
    assert!(!server.has_pending_connection());
    assert_eq!(channel.state(), ChannelState::Failed);
} // end fn a_cancelled_attempt_drops_the_provider_and_its_late_credentials_open_nothing

async fn connect_with_credentials(payload: &str, signature: &str) -> Result<(), CelerisError> {
    let credentials = Credentials {
        payload: payload.to_owned(),
        signature: signature.to_owned(),
    };

    let provider = CredentialProvider::new(move |_request| {
        let credentials = credentials.clone();

        async move { Ok(credentials) }
    });

    let (channel, server) = channel_for(options_with_provider(provider));
    let result = channel.connect().await;

    assert_eq!(channel.state(), ChannelState::Failed);
    assert_eq!(server.urls.len(), 0);

    result
} // end fn connect_with_credentials

#[tokio::test(start_paused = true)]
async fn credentials_with_empty_fields_are_a_configuration_error_naming_the_field() {
    assert_eq!(
        connect_with_credentials("", "").await,
        Err(CelerisError::Configuration(
            "Invalid credentials. payload: Must not be empty. signature: Must not be empty."
                .to_owned()
        ))
    );

    assert_eq!(
        connect_with_credentials("", "secret-signature").await,
        Err(CelerisError::Configuration(
            "Invalid credentials. payload: Must not be empty.".to_owned()
        ))
    );

    // The message names the field, never the value.
    assert_eq!(
        connect_with_credentials("secret-payload", "").await,
        Err(CelerisError::Configuration(
            "Invalid credentials. signature: Must not be empty.".to_owned()
        ))
    );
} // end fn credentials_with_empty_fields_are_a_configuration_error_naming_the_field

#[tokio::test(start_paused = true)]
async fn one_deadline_covers_the_credential_request_and_the_handshake() {
    let provider = CredentialProvider::new(|_request| async {
        sleep(Duration::from_secs(3)).await;

        Ok(test_credentials())
    });

    let options = ClientOptions {
        connect_timeout: Some(Duration::from_secs(5)),
        ..options_with_provider(provider)
    };

    let (channel, server) = channel_for(options);
    server.set_behaviour(DialBehaviour::Hang);
    let start = Instant::now();
    let connecting = channel.clone();
    let attempt = tokio::spawn(async move { connecting.connect().await });

    // The handshake starts once the credentials arrive, at 3 s.
    sleep(Duration::from_millis(2_999)).await;
    assert_eq!(server.urls.len(), 0);
    sleep(Duration::from_millis(1)).await;
    wait_until("the handshake", || server.urls.len() == 1).await;

    // It has 2 s of the 5 s left, not another 5 s.
    let result = attempt.await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    assert_eq!(
        result,
        Err(CelerisError::Timeout(
            "Connection attempt timed out after 5000 ms.".to_owned()
        ))
    );
    assert_eq!(channel.state(), ChannelState::Failed);
} // end fn one_deadline_covers_the_credential_request_and_the_handshake

#[tokio::test(start_paused = true)]
async fn a_cancellation_before_the_attempt_does_not_call_the_provider() {
    let (options, log) = test_options();
    let (channel, server) = channel_for(options);
    let states = state_recorder(&channel);
    let errors = error_recorder(&channel);

    // A connect future dropped before it runs starts nothing.
    drop(channel.connect());
    sleep(Duration::from_secs(1)).await;
    assert_eq!(log.requests.len(), 0);
    assert_eq!(channel.state(), ChannelState::Idle);

    // The control: a connect that runs asks once and connects.
    let (connected, _peer) = tokio::join!(channel.connect(), server.accept());
    connected.unwrap();
    settle(&channel).await;

    assert_eq!(log.requests.len(), 1);
    assert_eq!(
        states.all(),
        vec![ChannelState::Connecting, ChannelState::Connected]
    );
    assert_eq!(errors.all(), Vec::new());

    channel.close().await;
} // end fn a_cancellation_before_the_attempt_does_not_call_the_provider

#[tokio::test(start_paused = true)]
async fn a_connect_after_close_does_not_call_the_provider() {
    let (options, log) = test_options();
    let (channel, _server) = channel_for(options);

    channel.close().await;

    assert_eq!(
        channel.connect().await,
        Err(CelerisError::NotConnected(
            "Channel is closed; create a new one with Client::channel.".to_owned()
        ))
    );
    assert_eq!(log.requests.len(), 0);
} // end fn a_connect_after_close_does_not_call_the_provider
