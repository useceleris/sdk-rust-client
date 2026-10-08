//! Clients and channels for the live suites. Every channel that a test opens
//! is an [`Opened`], which closes it when the test ends, also when it fails.

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::sleep;
use useceleris_client::{
    Channel, Client, ClientOptions, CredentialProvider, CredentialRequest, Segment, Subscription,
};

use crate::support::collect::Recorder;
use crate::support::environment::websocket_url;
use crate::support::signer::{Claims, now_millis, sign};
use crate::support::wait::await_body;

static CHANNEL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A channel that closes when it is dropped. Its clones do not close it.
pub struct Opened {
    channel: Channel,
} // end struct Opened

impl Deref for Opened {
    type Target = Channel;

    fn deref(&self) -> &Channel {
        &self.channel
    } // end method deref
} // end impl Deref for Opened

impl Drop for Opened {
    fn drop(&mut self) {
        let Ok(runtime) = Handle::try_current() else {
            return;
        };

        let channel = self.channel.clone();
        tokio::task::block_in_place(move || runtime.block_on(channel.close()));
    } // end method drop
} // end impl Drop for Opened

/// A channel reference that no other test uses.
pub fn unique_channel_reference(label: &str) -> String {
    let count = CHANNEL_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;

    format!("rsqual-{label}-{}-{count}", now_millis())
} // end fn unique_channel_reference

/// A provider that signs `claims` afresh for every attempt.
pub fn signing_provider(claims: Claims) -> CredentialProvider {
    CredentialProvider::new(move |_request| {
        let credentials = sign(&claims);

        async move { Ok(credentials) }
    })
} // end fn signing_provider

/// A provider that records every request, and signs the claims that
/// `claims_for` picks for it.
pub fn recording_provider(
    claims_for: impl Fn(&CredentialRequest) -> Claims + Send + Sync + 'static,
) -> (CredentialProvider, Recorder<CredentialRequest>) {
    let requests = Recorder::default();
    let sink = requests.clone();

    let provider = CredentialProvider::new(move |request| {
        let credentials = sign(&claims_for(&request));
        sink.push(request);

        async move { Ok(credentials) }
    });

    (provider, requests)
} // end fn recording_provider

/// Options for `base_url`, which may be a loopback `ws://` URL.
pub fn options(base_url: &str, credential_provider: CredentialProvider) -> ClientOptions {
    ClientOptions {
        base_url: Some(base_url.to_owned()),
        allow_insecure_loopback: true,
        ..ClientOptions::new(credential_provider)
    }
} // end fn options

/// An unconnected channel.
pub fn open(options: ClientOptions, reference: &str) -> Opened {
    let client = Client::new(options).expect("the client options are valid");

    Opened {
        channel: client
            .channel(reference)
            .expect("the channel reference is valid"),
    }
} // end fn open

#[track_caller]
pub fn connect(channel: &Channel) -> impl Future<Output = ()> {
    let location = std::panic::Location::caller();
    let channel = channel.clone();

    async move {
        if let Err(error) = channel.connect().await {
            panic!("{location}: connect failed: {error}");
        }
    }
} // end fn connect

pub async fn connected(reference: &str, claims: Claims) -> Opened {
    connected_at(websocket_url(), reference, claims).await
} // end fn connected

pub async fn connected_at(base_url: &str, reference: &str, claims: Claims) -> Opened {
    let opened = open(options(base_url, signing_provider(claims)), reference);
    connect(&opened).await;

    opened
} // end fn connected_at

/// A publisher with the default claims and a receiver with `receiver_claims`,
/// on one new channel reference.
pub async fn pair(label: &str, receiver_claims: Claims) -> (Opened, Opened) {
    let reference = unique_channel_reference(label);
    let publisher = connected(&reference, Claims::default()).await;
    let receiver = connected(&reference, receiver_claims).await;

    (publisher, receiver)
} // end fn pair

pub fn segment(channel: &Channel, segment_id: &str) -> Segment {
    channel
        .segment(segment_id)
        .expect("the segment id is valid")
} // end fn segment

pub fn subscribe(segment: &Segment) -> Subscription {
    segment.subscribe().expect("the channel is open")
} // end fn subscribe

pub fn subscribe_presence(segment: &Segment) -> Subscription {
    segment.subscribe_presence().expect("the channel is open")
} // end fn subscribe_presence

#[track_caller]
pub fn publish_text(channel: &Channel, segment_id: &str, body: &str) -> impl Future<Output = ()> {
    let location = std::panic::Location::caller();
    let target = segment(channel, segment_id);
    let body = body.to_owned();

    async move {
        if let Err(error) = target.publish(body.as_bytes()).await {
            panic!("{location}: publish of {body:?} failed: {error}");
        }
    }
} // end fn publish_text

pub async fn publish_all(channel: &Channel, segment_id: &str, bodies: &[&str]) {
    for body in bodies {
        publish_text(channel, segment_id, body).await;
    }
} // end fn publish_all

/// Publishes `body` and waits until the receiver's segment delivers it.
pub async fn publish_and_await(
    publisher: &Channel,
    receiver: &Channel,
    segment_id: &str,
    body: &str,
) {
    let arrived = await_body(&segment(receiver, segment_id), body);
    publish_text(publisher, segment_id, body).await;
    arrived.wait().await;
} // end fn publish_and_await

/// Proves that the receiver was live before a check that nothing arrived: a
/// control message on the default segment, which always delivers, then time
/// for a stray delivery to land.
pub async fn confirm_quiet(publisher: &Channel, receiver: &Channel) {
    publish_and_await(publisher, receiver, "default", "control").await;
    sleep(Duration::from_millis(2500)).await;
} // end fn confirm_quiet

/// Publishes paced in batches of 10, below the per-second publish limit.
pub async fn publish_paced(channel: &Channel, segment_id: &str, bodies: &[&str]) {
    for batch in bodies.chunks(10) {
        publish_all(channel, segment_id, batch).await;
        sleep(Duration::from_millis(1100)).await;
    }
} // end fn publish_paced
