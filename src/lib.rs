//! The Celeris realtime client.
//!
//! One [`Channel`] is one WebSocket connection; its segments share that
//! connection. Credentials come from your own server through a
//! [`CredentialProvider`], fresh for every connection attempt.
//!
//! ```no_run
//! use useceleris_client::{Client, ClientOptions, CredentialProvider, Credentials, read_text, text_payload};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = CredentialProvider::new(|request| async move {
//!     // Ask your own server to sign credentials for request.channel_reference.
//!     # let _ = request;
//!     Ok(Credentials { payload: String::new(), signature: String::new() })
//! });
//!
//! let client = Client::new(ClientOptions::new(provider))?;
//! let channel = client.channel("room-1")?;
//! let chat = channel.segment("chat")?;
//!
//! chat.on_message(|payload, metadata| {
//!     println!("{}: {:?}", metadata.token_reference, read_text(payload));
//! });
//!
//! let _subscription = chat.subscribe()?;
//! channel.connect().await?;
//! chat.publish(text_payload("hello")).await?;
//! channel.close().await;
//! # Ok(())
//! # }
//! ```

mod channel;
mod client;
mod command_queue;
mod connection;
mod connection_url;
mod constants;
mod credentials;
mod decode;
mod encode;
mod errors;
mod events;
mod heartbeat;
mod identifiers;
mod messages;
mod ordered_map;
mod payload;
mod random;
mod reconnect;
mod routing;
mod segment;
mod values;

#[cfg(test)]
mod tests;

/// Compiles every code block of the README, so it cannot drift from the API.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;

pub use channel::Channel;
pub use client::{Client, ClientOptions};
pub use credentials::{
    CredentialFuture, CredentialProvider, CredentialReason, CredentialRequest, Credentials,
    ProviderError,
};
pub use errors::{CelerisError, ServerError, ServerErrorResource, ServerErrorType};
pub use events::{ChannelEventHandler, Registration};
pub use payload::{PayloadCodec, read_text, text_payload};
pub use segment::{Segment, Subscription};
pub use values::{
    ChannelState, MessageMetadata, PresenceConnection, PresenceEvent, PresencePage, RecoveryEvent,
    ServerNotice,
};
