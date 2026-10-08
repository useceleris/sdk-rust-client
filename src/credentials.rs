use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::errors::{CelerisError, configuration_error, failure};

/// Authorizes one connection attempt. Both values are opaque: the SDK never
/// parses, re-serializes or generates them. A trusted server signs them; a
/// client only passes them on.
///
/// `Debug` shows them as redacted, so they never reach a log by accident.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub payload: String,
    pub signature: String,
} // end struct Credentials

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Credentials { redacted }")
    } // end method fmt
} // end impl Debug for Credentials

/// Why credentials are requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialReason {
    /// The attempt that [`Channel::connect`](crate::Channel::connect) starts.
    Initial,

    /// An automatic reconnect.
    Reconnect {
        /// When the connection dropped.
        disconnected_at: SystemTime,

        /// A suggestion for how much history to replay: the outage so far plus
        /// five seconds of overlap. The signing server decides if it signs any
        /// replay into the credentials.
        replay_lookback: Duration,
    },
} // end enum CredentialReason

/// Describes the attempt that credentials are requested for. Credentials are
/// requested fresh for every attempt, reconnects included (D-001).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialRequest {
    pub channel_reference: String,
    pub reason: CredentialReason,
} // end struct CredentialRequest

/// Any error that a credential provider returns. The SDK never inspects it or
/// passes it on.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync>;

/// The future that a credential provider returns.
pub type CredentialFuture =
    Pin<Box<dyn Future<Output = Result<Credentials, ProviderError>> + Send>>;

/// Fetches credentials for one attempt, typically from your own credential
/// endpoint.
///
/// The SDK drops the provider's future when the attempt's deadline passes or
/// the channel closes, and discards anything that it would return afterwards.
/// An error or a panic is reported as [`CelerisError::Transport`]; the SDK
/// never inspects or passes on its detail.
#[derive(Clone)]
pub struct CredentialProvider {
    provide: Arc<dyn Fn(CredentialRequest) -> CredentialFuture + Send + Sync>,
} // end struct CredentialProvider

impl CredentialProvider {
    /// Wraps an async function or closure:
    ///
    /// ```
    /// use useceleris_client::{CredentialProvider, Credentials};
    ///
    /// let provider = CredentialProvider::new(|_request| async {
    ///     // Fetch these from your own credential endpoint.
    ///     Ok(Credentials {
    ///         payload: "payload".to_owned(),
    ///         signature: "signature".to_owned(),
    ///     })
    /// });
    /// ```
    pub fn new<Provide, Provided>(provide: Provide) -> CredentialProvider
    where
        Provide: Fn(CredentialRequest) -> Provided + Send + Sync + 'static,
        Provided: Future<Output = Result<Credentials, ProviderError>> + Send + 'static,
    {
        CredentialProvider {
            provide: Arc::new(move |request| Box::pin(provide(request))),
        }
    } // end fn new

    /// Calls the provider.
    pub fn provide(&self, request: CredentialRequest) -> CredentialFuture {
        (self.provide)(request)
    } // end method provide
} // end impl CredentialProvider

impl fmt::Debug for CredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialProvider")
    } // end method fmt
} // end impl Debug for CredentialProvider

/// Checks what a provider returned, naming the field that failed but never its
/// value. Rust text is always valid UTF-8, so only emptiness remains to check
/// (D-003).
pub(crate) fn validate_credentials(credentials: &Credentials) -> Result<(), CelerisError> {
    let mut failures = Vec::new();

    if credentials.payload.is_empty() {
        failures.push(failure("payload", "Must not be empty"));
    }

    if credentials.signature.is_empty() {
        failures.push(failure("signature", "Must not be empty"));
    }

    if !failures.is_empty() {
        return Err(configuration_error("credentials", &failures));
    }

    Ok(())
} // end fn validate_credentials
