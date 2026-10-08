//! Payloads are opaque bytes on the wire. These helpers cover text; every
//! other format, JSON included, goes through [`PayloadCodec`], which keeps
//! serializer libraries out of this crate (HELP-01).

use std::fmt;
use std::sync::Arc;

use crate::errors::CelerisError;

/// Encodes text as UTF-8.
pub fn text_payload(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
} // end fn text_payload

/// Decodes a UTF-8 payload. It fails with [`CelerisError::Configuration`] when
/// the payload is not valid UTF-8.
pub fn read_text(payload: &[u8]) -> Result<String, CelerisError> {
    match std::str::from_utf8(payload) {
        Ok(text) => Ok(text.to_owned()),
        Err(_) => Err(CelerisError::Configuration(
            "Payload is not valid UTF-8, so it cannot be read as text.".to_owned(),
        )),
    }
} // end fn read_text

type Encode<Value, Failure> = dyn Fn(&Value) -> Result<Vec<u8>, Failure> + Send + Sync;

type Decode<Value, Failure> = dyn Fn(&[u8]) -> Result<Value, Failure> + Send + Sync;

/// Gives a serializer of your choice, such as JSON, MessagePack or protobuf,
/// one shape for both directions. The encoder's and the decoder's own errors
/// are returned unchanged: they are yours, not this crate's.
///
/// ```
/// use useceleris_client::PayloadCodec;
///
/// // With serde_json, this is
/// // PayloadCodec::new(serde_json::to_vec, |payload| serde_json::from_slice(payload)).
/// let numbers = PayloadCodec::new(
///     |value: &u32| Ok::<_, std::num::ParseIntError>(value.to_string().into_bytes()),
///     |payload: &[u8]| String::from_utf8_lossy(payload).parse::<u32>(),
/// );
///
/// let payload = numbers.encode_payload(&42).unwrap();
/// assert_eq!(numbers.read_payload(&payload).unwrap(), 42);
/// ```
pub struct PayloadCodec<Value, Failure> {
    encode: Arc<Encode<Value, Failure>>,
    decode: Arc<Decode<Value, Failure>>,
} // end struct PayloadCodec

impl<Value, Failure> PayloadCodec<Value, Failure> {
    /// Wraps an encoder and a decoder.
    pub fn new(
        encode: impl Fn(&Value) -> Result<Vec<u8>, Failure> + Send + Sync + 'static,
        decode: impl Fn(&[u8]) -> Result<Value, Failure> + Send + Sync + 'static,
    ) -> PayloadCodec<Value, Failure> {
        PayloadCodec {
            encode: Arc::new(encode),
            decode: Arc::new(decode),
        }
    } // end fn new

    /// Encodes `value` with the codec's encoder.
    pub fn encode_payload(&self, value: &Value) -> Result<Vec<u8>, Failure> {
        (self.encode)(value)
    } // end method encode_payload

    /// Decodes `payload` with the codec's decoder.
    pub fn read_payload(&self, payload: &[u8]) -> Result<Value, Failure> {
        (self.decode)(payload)
    } // end method read_payload
} // end impl PayloadCodec

impl<Value, Failure> Clone for PayloadCodec<Value, Failure> {
    fn clone(&self) -> PayloadCodec<Value, Failure> {
        PayloadCodec {
            encode: Arc::clone(&self.encode),
            decode: Arc::clone(&self.decode),
        }
    } // end method clone
} // end impl Clone for PayloadCodec

impl<Value, Failure> fmt::Debug for PayloadCodec<Value, Failure> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PayloadCodec")
    } // end method fmt
} // end impl Debug for PayloadCodec
