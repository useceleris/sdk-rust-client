//! Connects to Celeris, publishes on a segment, and reads who is present.
//!
//! A trusted server signs the credentials. This client gets them from your
//! credential endpoint. The client never sees a signing secret.
//!
//! To run this example:
//!
//! 1. Change `fetch_credentials` so that it calls your credential endpoint.
//! 2. Set CELERIS_WS_URL to your Celeris endpoint.
//! 3. Run `cargo run --example quickstart`.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use useceleris_client::{
    Client, ClientOptions, CredentialProvider, CredentialRequest, Credentials, ProviderError,
    read_text, text_payload,
};

/// The result of one run.
pub struct Summary {
    /// The number of messages that the example received.
    pub delivered: usize,

    /// The number of connections in the segment.
    pub present: usize,
} // end struct Summary

/// Your credential endpoint. Replace the body of this function.
///
/// The SDK calls it for each connection attempt, and for each reconnect.
/// Send `request.channel_reference` to your own server. Your server makes
/// sure that the user is signed in, decides the claims, and signs them. Then
/// return the payload and the signature that your server sends back.
async fn fetch_credentials(request: CredentialRequest) -> Result<Credentials, ProviderError> {
    let _ = request.channel_reference;

    Err("Change fetch_credentials so that it calls your credential endpoint.".into())
} // end fn fetch_credentials

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let base_url = std::env::var("CELERIS_WS_URL")?;
    let summary = run(&base_url, CredentialProvider::new(fetch_credentials)).await?;

    println!(
        "example: ok delivered={} present={}",
        summary.delivered, summary.present
    );

    Ok(())
} // end fn main

/// Connects, publishes one message, receives it, and lists the segment.
pub async fn run(
    base_url: &str,
    credential_provider: CredentialProvider,
) -> Result<Summary, Box<dyn Error + Send + Sync>> {
    let client = Client::new(ClientOptions {
        base_url: Some(base_url.to_owned()),
        // This permits a local ws:// stack. Production uses wss://.
        allow_insecure_loopback: true,
        ..ClientOptions::new(credential_provider)
    })?;

    let started = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let channel = client.channel(&format!("quickstart-{started}"))?;
    channel
        .events()
        .on_state_change(|state| println!("state: {state}"));
    channel
        .events()
        .on_error(|error| println!("error: {error}"));

    let chat = channel.segment("chat")?;
    let delivered = Arc::new(AtomicUsize::new(0));
    let arrived = Arc::new(Notify::new());
    let (counter, signal) = (Arc::clone(&delivered), Arc::clone(&arrived));

    // The listener gets the payload first. The metadata tells the sender, the
    // message id and the time.
    chat.on_message(move |payload, metadata| {
        let text = read_text(payload).unwrap_or_default();
        println!("received {} {text}", metadata.message_id);
        counter.fetch_add(1, Ordering::SeqCst);
        signal.notify_one();
    });

    let subscription = chat.subscribe()?;

    // Close the channel on every path, also when a step fails.
    let outcome = async {
        channel.connect().await?;
        chat.publish(text_payload("hello")).await?;
        tokio::time::timeout(Duration::from_secs(15), arrived.notified()).await?;
        let page = chat.presence_list(1, 100).await?;

        for connection in &page.connections {
            println!("present: {}", connection.token_reference);
        }

        Ok::<Summary, Box<dyn Error + Send + Sync>>(Summary {
            delivered: delivered.load(Ordering::SeqCst),
            present: page.connections.len(),
        })
    }
    .await;

    subscription.cancel();
    channel.close().await;

    outcome
} // end fn run
