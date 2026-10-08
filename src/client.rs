use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::Channel;
use crate::connection::{Dialer, web_socket_dialer};
use crate::connection_url::validate_base_url;
use crate::constants::{
    DEFAULT_BASE_URL, DEFAULT_CONNECT_TIMEOUT, DEFAULT_DEDUPLICATION_WINDOW_SIZE,
    DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS, DEFAULT_PRESENCE_QUERY_TIMEOUT, MAXIMUM_PENDING_COMMANDS,
    MAXIMUM_RECONNECT_ATTEMPTS_CEILING, MAXIMUM_TIMEOUT, MINIMUM_TIMEOUT,
};
use crate::credentials::CredentialProvider;
use crate::errors::{CelerisError, configuration_error, failure};
use crate::identifiers::channel_reference_rule;
use crate::random::random_fraction;

/// Configures a [`Client`]. Only the credential provider is required; `None`
/// keeps an option's default (CONFIG-01).
///
/// ```
/// use std::time::Duration;
/// use useceleris_client::{ClientOptions, CredentialProvider, Credentials};
///
/// # let provider = CredentialProvider::new(|_| async { Ok(Credentials { payload: "p".into(), signature: "s".into() }) });
/// let options = ClientOptions {
///     connect_timeout: Some(Duration::from_secs(5)),
///     ..ClientOptions::new(provider)
/// };
/// ```
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Fetches fresh credentials for every connection attempt.
    pub credential_provider: CredentialProvider,

    /// Defaults to the production endpoint. Set it only for local, CI or
    /// self-hosted targets.
    pub base_url: Option<String>,

    /// Permits a `ws://` base URL for a loopback host.
    pub allow_insecure_loopback: bool,

    /// Covers credential acquisition and the handshake together: 1 ms to 15
    /// minutes, 15 seconds by default.
    pub connect_timeout: Option<Duration>,

    /// The same deadline for each reconnect attempt: 1 ms to 15 minutes, the
    /// connect timeout by default.
    pub reconnect_timeout: Option<Duration>,

    /// Bounds each presence query: 1 ms to 15 minutes, 10 seconds by default.
    pub presence_query_timeout: Option<Duration>,

    /// Caps the publishes that wait for room in the writer: at least 1, 64 by
    /// default.
    pub publish_queue_size: Option<usize>,

    /// How many delivered message ids each channel remembers, to drop replayed
    /// duplicates: at least 1, 1024 by default.
    pub deduplication_window_size: Option<usize>,

    /// How many failed reconnect attempts, in one retry budget, end recovery in
    /// [`ChannelState::Failed`](crate::ChannelState::Failed): 1 to 100, 10 by
    /// default.
    pub maximum_reconnect_attempts: Option<u32>,
} // end struct ClientOptions

impl ClientOptions {
    /// Options with every default and the given provider.
    pub fn new(credential_provider: CredentialProvider) -> ClientOptions {
        ClientOptions {
            credential_provider,
            base_url: None,
            allow_insecure_loopback: false,
            connect_timeout: None,
            reconnect_timeout: None,
            presence_query_timeout: None,
            publish_queue_size: None,
            deduplication_window_size: None,
            maximum_reconnect_attempts: None,
        }
    } // end fn new
} // end impl ClientOptions

/// Creates channels. It holds configuration only and does no network work.
/// It is cheap to clone.
#[derive(Clone)]
pub struct Client {
    pub(crate) configuration: Arc<ClientConfiguration>,
} // end struct Client

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("base_url", &self.configuration.base_url)
            .finish_non_exhaustive()
    } // end method fmt
} // end impl Debug for Client

/// A client's validated options, with every default applied.
#[derive(Clone)]
pub(crate) struct ClientConfiguration {
    pub(crate) credential_provider: CredentialProvider,
    pub(crate) base_url: String,
    pub(crate) connect_timeout: Duration,
    pub(crate) reconnect_timeout: Duration,
    pub(crate) presence_query_timeout: Duration,
    pub(crate) publish_queue_size: usize,
    pub(crate) deduplication_window_size: usize,
    pub(crate) maximum_reconnect_attempts: u32,

    /// The seams: tests substitute an in-memory peer and fix the jitter.
    pub(crate) dial: Dialer,
    pub(crate) random: Arc<dyn Fn() -> f64 + Send + Sync>,
} // end struct ClientConfiguration

impl Client {
    /// Validates `options` and returns a client. It fails with
    /// [`CelerisError::Configuration`], which names every field that failed
    /// and its rule: a timeout outside 1 ms to 15 minutes, a size of zero, a
    /// maximum reconnect attempts outside 1 to 100, or a base URL that is not
    /// `wss://`, or `ws://` for a loopback host when allowed.
    pub fn new(options: ClientOptions) -> Result<Client, CelerisError> {
        const TIMEOUT_RULE: &str = "Must be from 1 ms to 15 minutes";
        const SIZE_RULE: &str = "Must be at least 1";

        let mut failures = Vec::new();

        let timeouts = [
            ("connect_timeout", options.connect_timeout),
            ("reconnect_timeout", options.reconnect_timeout),
            ("presence_query_timeout", options.presence_query_timeout),
        ];

        for (field, timeout) in timeouts {
            if timeout
                .is_some_and(|timeout| !(MINIMUM_TIMEOUT..=MAXIMUM_TIMEOUT).contains(&timeout))
            {
                failures.push(failure(field, TIMEOUT_RULE));
            }
        }

        let sizes = [
            ("publish_queue_size", options.publish_queue_size),
            (
                "deduplication_window_size",
                options.deduplication_window_size,
            ),
        ];

        for (field, size) in sizes {
            if size == Some(0) {
                failures.push(failure(field, SIZE_RULE));
            }
        }

        let attempts_valid =
            |attempts| (1..=MAXIMUM_RECONNECT_ATTEMPTS_CEILING).contains(&attempts);

        if options
            .maximum_reconnect_attempts
            .is_some_and(|attempts| !attempts_valid(attempts))
        {
            failures.push(failure(
                "maximum_reconnect_attempts",
                "Must be from 1 to 100",
            ));
        }

        if !failures.is_empty() {
            return Err(configuration_error("client options", &failures));
        }

        let base_url = validate_base_url(
            options.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL),
            options.allow_insecure_loopback,
        )?;

        let connect_timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let configuration = ClientConfiguration {
            credential_provider: options.credential_provider,
            base_url,
            connect_timeout,
            reconnect_timeout: options.reconnect_timeout.unwrap_or(connect_timeout),
            presence_query_timeout: options
                .presence_query_timeout
                .unwrap_or(DEFAULT_PRESENCE_QUERY_TIMEOUT),
            publish_queue_size: options
                .publish_queue_size
                .unwrap_or(MAXIMUM_PENDING_COMMANDS),
            deduplication_window_size: options
                .deduplication_window_size
                .unwrap_or(DEFAULT_DEDUPLICATION_WINDOW_SIZE),
            maximum_reconnect_attempts: options
                .maximum_reconnect_attempts
                .unwrap_or(DEFAULT_MAXIMUM_RECONNECT_ATTEMPTS),
            dial: web_socket_dialer(),
            random: Arc::new(random_fraction),
        };

        Ok(Client {
            configuration: Arc::new(configuration),
        })
    } // end fn new

    /// A handle for the channel with the given reference. It does no network
    /// work; each call returns a new channel with its own connection. It fails
    /// with [`CelerisError::Configuration`] unless the reference is 1 to 255
    /// ASCII letters, digits, hyphens or underscores.
    pub fn channel(&self, reference: &str) -> Result<Channel, CelerisError> {
        if let Some(rule) = channel_reference_rule(reference) {
            return Err(configuration_error(
                "channel reference",
                &[failure("", rule)],
            ));
        }

        Ok(Channel::new(Arc::clone(&self.configuration), reference))
    } // end method channel
} // end impl Client
