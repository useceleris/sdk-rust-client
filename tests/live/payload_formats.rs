//! Payload formats (catalogue sections 4 and 15). Payloads are opaque bytes
//! to the SDK and the server: whatever a caller encodes is what the peer
//! decodes. The vectors are written by hand, so the suite needs no
//! serializer, and each is decoded on arrival.

use std::time::Duration;

use useceleris_client::{PayloadCodec, read_text, text_payload};

use crate::support::channels::{pair, segment, subscribe};
use crate::support::collect::{collect, is_generated_message_id};
use crate::support::signer::Claims;
use crate::support::wait::{await_message, eventually, settle};

/// protobuf: field 1 varint 150, field 2 "안녕 celeris", field 3 { field 1 = 1 }.
const PROTOBUF_VECTOR: [u8; 23] = [
    0x08, 0x96, 0x01, 0x12, 0x0e, 0xec, 0x95, 0x88, 0xeb, 0x85, 0x95, 0x20, 0x63, 0x65, 0x6c, 0x65,
    0x72, 0x69, 0x73, 0x1a, 0x02, 0x08, 0x01,
];

/// MessagePack: fixmap(3) { "id": 7, "bin": bin8 <00 ff 10>, "txt": "héllo" }.
const MESSAGE_PACK_VECTOR: [u8; 25] = [
    0x83, 0xa2, 0x69, 0x64, 0x07, 0xa3, 0x62, 0x69, 0x6e, 0xc4, 0x03, 0x00, 0xff, 0x10, 0xa3, 0x74,
    0x78, 0x74, 0xa6, 0x68, 0xc3, 0xa9, 0x6c, 0x6c, 0x6f,
];

/// The JSON that `JSON.stringify` writes for the reference's value.
const JSON_VECTOR: &str = "{\"id\":7,\"txt\":\"héllo 안녕\",\"nested\":{\"ok\":true}}";

/// Every byte value, then CR and LF in both orders.
fn binary_vector() -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..=255).collect();
    bytes.extend_from_slice(b"\r\n\n\r");

    bytes
} // end fn binary_vector

/// Reads a protobuf varint at `offset`; returns it and the next offset.
fn read_varint(bytes: &[u8], mut offset: usize) -> (u64, usize) {
    let mut value = 0;
    let mut shift = 0;

    loop {
        let byte = bytes[offset];
        offset += 1;
        value |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return (value, offset);
        }

        shift += 7;
    }
} // end fn read_varint

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_trips_payloads_byte_identically() {
    let vectors = [
        ("json", JSON_VECTOR.as_bytes().to_vec()),
        ("messagepack", MESSAGE_PACK_VECTOR.to_vec()),
        ("protobuf", PROTOBUF_VECTOR.to_vec()),
        ("binary", binary_vector()),
    ];

    for (label, vector) in vectors {
        let (publisher, receiver) = pair(&format!("fmt-{label}"), Claims::default()).await;
        let formats = segment(&receiver, "formats");
        subscribe(&formats);
        settle().await;

        let arrived = await_message(
            &formats,
            |_| true,
            &format!("the {label} delivery"),
            Duration::from_secs(20),
        );

        segment(&publisher, "formats")
            .publish(&vector)
            .await
            .expect("the publish is accepted");

        let message = arrived.wait().await;

        assert_eq!(
            message.payload, vector,
            "{label}: the payload changed in transit"
        );
        assert!(
            is_generated_message_id(&message.metadata.message_id),
            "{label}: message id {:?}",
            message.metadata.message_id
        );
    }
} // end fn round_trips_payloads_byte_identically

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_payloads_that_decode_to_their_intended_values() {
    let (publisher, receiver) = pair("fmt-decode", Claims::default()).await;
    let formats = segment(&receiver, "formats");
    let received = collect(&formats);
    subscribe(&formats);
    settle().await;

    for vector in [
        JSON_VECTOR.as_bytes(),
        &MESSAGE_PACK_VECTOR,
        &PROTOBUF_VECTOR,
    ] {
        segment(&publisher, "formats")
            .publish(vector)
            .await
            .expect("the publish is accepted");
    }

    eventually(
        || received.count() >= 3,
        "all three format deliveries",
        Duration::from_secs(20),
    )
    .await;

    // Deliveries from one origin keep their order.
    let deliveries = received.all();

    assert_eq!(
        read_text(&deliveries[0].payload).expect("the JSON is UTF-8"),
        JSON_VECTOR
    );

    // MessagePack: a fixmap header, then the bin8 field's exact bytes.
    let message_pack = &deliveries[1].payload;
    let binary_start = message_pack
        .iter()
        .position(|byte| *byte == 0xc4)
        .expect("a bin8 field");

    assert_eq!(message_pack[0] & 0xf0, 0x80);
    assert_eq!(
        message_pack[binary_start..binary_start + 5],
        [0xc4, 0x03, 0x00, 0xff, 0x10]
    );

    // protobuf: field 1 is a varint that carries 150, field 2 is the string.
    let protobuf = &deliveries[2].payload;
    let (field_one, after_field_one) = read_varint(protobuf, 1);
    let (length, after_length) = read_varint(protobuf, after_field_one + 1);
    let length = usize::try_from(length).expect("a small length");

    assert_eq!(protobuf[0], 0x08);
    assert_eq!(field_one, 150);
    assert_eq!(protobuf[after_field_one], 0x12);
    assert_eq!(
        read_text(&protobuf[after_length..after_length + length]).expect("the string is UTF-8"),
        "안녕 celeris"
    );
} // end fn delivers_payloads_that_decode_to_their_intended_values

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn carries_helper_and_codec_payloads_through_the_live_server() {
    let (publisher, receiver) = pair("fmt-helpers", Claims::default()).await;
    let formats = segment(&receiver, "formats");
    let received = collect(&formats);
    subscribe(&formats);
    settle().await;

    // The codec wraps any serializer; here, the protobuf vector.
    let vector_codec = PayloadCodec::new(
        |_marker: &u8| Ok::<_, String>(PROTOBUF_VECTOR.to_vec()),
        |payload: &[u8]| payload.first().copied().ok_or_else(|| "empty".to_owned()),
    );

    let publishing = segment(&publisher, "formats");
    let encoded = vector_codec.encode_payload(&8).expect("the codec encodes");

    for payload in [text_payload("hi"), encoded] {
        publishing
            .publish(payload)
            .await
            .expect("the publish is accepted");
    }

    eventually(
        || received.count() >= 2,
        "the two helper deliveries",
        Duration::from_secs(20),
    )
    .await;

    let deliveries = received.all();

    assert_eq!(read_text(&deliveries[0].payload), Ok("hi".to_owned()));
    assert_eq!(vector_codec.read_payload(&deliveries[1].payload), Ok(8));
    assert_eq!(deliveries[1].payload, PROTOBUF_VECTOR);
} // end fn carries_helper_and_codec_payloads_through_the_live_server
