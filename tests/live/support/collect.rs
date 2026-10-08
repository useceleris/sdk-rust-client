//! Thread-safe collectors. Listeners run on Tokio's blocking pool, so every
//! value goes through a mutex.

use std::sync::{Arc, Mutex, PoisonError};

use useceleris_client::{
    Channel, CredentialReason, CredentialRequest, MessageMetadata, PresenceEvent, Registration,
    Segment,
};

/// One delivery: its payload and its metadata.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub payload: Vec<u8>,
    pub metadata: MessageMetadata,
} // end struct Delivery

impl Delivery {
    pub fn new(payload: &[u8], metadata: &MessageMetadata) -> Delivery {
        Delivery {
            payload: payload.to_vec(),
            metadata: metadata.clone(),
        }
    } // end fn new

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    } // end method text
} // end impl Delivery

/// Records every value that a listener gives it.
pub struct Recorder<Value> {
    values: Arc<Mutex<Vec<Value>>>,
} // end struct Recorder

impl<Value> Clone for Recorder<Value> {
    fn clone(&self) -> Recorder<Value> {
        Recorder {
            values: Arc::clone(&self.values),
        }
    } // end method clone
} // end impl Clone for Recorder

impl<Value> Default for Recorder<Value> {
    fn default() -> Recorder<Value> {
        Recorder {
            values: Arc::new(Mutex::new(Vec::new())),
        }
    } // end fn default
} // end impl Default for Recorder

impl<Value: Clone> Recorder<Value> {
    pub fn push(&self, value: Value) {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value);
    } // end method push

    pub fn all(&self) -> Vec<Value> {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    } // end method all

    pub fn count(&self) -> usize {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    } // end method count
} // end impl Recorder

/// Records every delivery of a segment.
pub fn collect(segment: &Segment) -> Recorder<Delivery> {
    let recorder = Recorder::default();
    let sink = recorder.clone();
    segment.on_message(move |payload, metadata| sink.push(Delivery::new(payload, metadata)));

    recorder
} // end fn collect

/// Records every delivery that the channel listener gets.
pub fn collect_channel(channel: &Channel) -> (Recorder<Delivery>, Registration) {
    let recorder = Recorder::default();
    let sink = recorder.clone();

    let registration = channel
        .events()
        .on_message(move |payload, metadata| sink.push(Delivery::new(payload, metadata)));

    (recorder, registration)
} // end fn collect_channel

/// Records every presence event of a segment.
pub fn record_presence(segment: &Segment) -> Recorder<PresenceEvent> {
    let recorder = Recorder::default();
    let sink = recorder.clone();
    segment.on_presence(move |event| sink.push(event.clone()));

    recorder
} // end fn record_presence

pub fn payloads(recorder: &Recorder<Delivery>) -> Vec<String> {
    recorder.all().iter().map(Delivery::text).collect()
} // end fn payloads

/// Each delivery as `segment:payload`.
pub fn tagged(recorder: &Recorder<Delivery>) -> Vec<String> {
    let deliveries = recorder.all();

    deliveries
        .iter()
        .map(|delivery| format!("{}:{}", delivery.metadata.segment_id, delivery.text()))
        .collect()
} // end fn tagged

/// The number of different message ids among the deliveries.
pub fn distinct_message_ids(recorder: &Recorder<Delivery>) -> usize {
    let mut message_ids: Vec<String> = recorder
        .all()
        .into_iter()
        .map(|delivery| delivery.metadata.message_id)
        .collect();

    message_ids.sort();
    message_ids.dedup();
    message_ids.len()
} // end fn distinct_message_ids

#[track_caller]
pub fn assert_payloads(recorder: &Recorder<Delivery>, expected: &[&str]) {
    assert_eq!(payloads(recorder), expected, "the received payloads");
} // end fn assert_payloads

/// The SDK gives every publish its own id, which the server delivers as it
/// is (RESEND-01): `msg__rust_v`, the crate version, `__`, then 16 random
/// bytes as hex.
pub fn is_generated_message_id(message_id: &str) -> bool {
    let prefix = concat!("msg__rust_v", env!("CARGO_PKG_VERSION"), "__");

    message_id.strip_prefix(prefix).is_some_and(|random| {
        random.len() == 32
            && random
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
} // end fn is_generated_message_id

/// Bytes that repeat 0 to 250.
pub fn patterned(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| u8::try_from(index % 251).expect("below 251"))
        .collect()
} // end fn patterned

/// `m0`, `m1`, ... up to `count` bodies.
pub fn numbered(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{prefix}{index}")).collect()
} // end fn numbered

/// The requests made for automatic reconnects.
pub fn reconnect_requests(requests: &Recorder<CredentialRequest>) -> Vec<CredentialRequest> {
    requests
        .all()
        .into_iter()
        .filter(|request| matches!(request.reason, CredentialReason::Reconnect { .. }))
        .collect()
} // end fn reconnect_requests
