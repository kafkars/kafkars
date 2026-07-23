//! Produce materialization delegates DTO layout and `RecordBatch` v2 bytes.

#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire::{OutboundFrameLimits, PRODUCE_API_DESCRIPTOR, encode_request};
use kafka_wire_records::{
    Compression, RecordBatch, RecordDecodeLimits, RecordError, TimestampType,
};

use super::produce::{
    ExplicitProduce, MaterializedProduce, ProduceHeader, materialize_explicit_produce,
};

const TOPIC: &str = "orders";
const PARTITION: i32 = 7;
const TIMESTAMP_MS: i64 = 1_725_000_000_123;
const REMAINING_BROKER_TIMEOUT_MS: i32 = 30_000;

#[test]
fn one_explicit_record_becomes_one_acks_all_generated_request() {
    let materialized = materialize_explicit_produce(input(usize::MAX)).unwrap();
    let request = materialized.request();

    assert!(request.transactional_id.is_none());
    assert_eq!(request.acks, -1);
    assert_eq!(request.timeout_ms, REMAINING_BROKER_TIMEOUT_MS);
    assert_eq!(request.topic_data.len(), 1);
    assert_eq!(request.topic_data[0].name.as_ref(), TOPIC);
    assert_eq!(request.topic_data[0].partition_data.len(), 1);
    assert_eq!(request.topic_data[0].partition_data[0].index, PARTITION);
}

#[test]
fn record_bytes_decode_as_the_wire_owned_uncompressed_v2_batch() {
    let materialized = materialize_explicit_produce(input(usize::MAX)).unwrap();
    let mut encoded = materialized.request().topic_data[0].partition_data[0]
        .records
        .clone()
        .unwrap();
    let retained = encoded.len();
    let batch = RecordBatch::decode(&mut encoded, RecordDecodeLimits::default()).unwrap();

    assert_eq!(materialized.retained_record_bytes(), retained);
    assert_eq!(batch.compression, Compression::None);
    assert_eq!(batch.timestamp_type, TimestampType::CreateTime);
    assert_eq!(batch.base_timestamp, TIMESTAMP_MS);
    assert_eq!(batch.max_timestamp, TIMESTAMP_MS);
    assert_eq!(batch.producer_id, -1);
    assert_eq!(batch.producer_epoch, -1);
    assert_eq!(batch.base_sequence, -1);
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].key.as_deref(), Some(&b"customer-42"[..]));
    assert_eq!(batch.records[0].value.as_deref(), Some(&b"created"[..]));
    assert!(encoded.is_empty());
}

#[test]
fn null_and_empty_payloads_remain_distinct_at_the_wire_boundary() {
    let null = materialize_explicit_produce(payload_input(None, None)).unwrap();
    let empty = materialize_explicit_produce(payload_input(Some(Bytes::new()), Some(Bytes::new())))
        .unwrap();
    let null = decoded_batch(&null);
    let empty = decoded_batch(&empty);

    assert!(null.records[0].key.is_none());
    assert!(null.records[0].value.is_none());
    assert_eq!(empty.records[0].key.as_deref(), Some(&b""[..]));
    assert_eq!(empty.records[0].value.as_deref(), Some(&b""[..]));
}

#[test]
fn headers_preserve_order_duplicates_and_nullable_values() {
    let headers = vec![
        ProduceHeader::new("traceparent".to_owned(), Some(Bytes::from_static(b"first"))),
        ProduceHeader::new("traceparent".to_owned(), None),
        ProduceHeader::new("traceparent".to_owned(), Some(Bytes::new())),
    ];
    let materialized =
        materialize_explicit_produce(input(usize::MAX).with_headers(headers)).unwrap();
    let batch = decoded_batch(&materialized);
    let headers = &batch.records[0].headers;

    assert_eq!(headers.len(), 3);
    assert_eq!(headers[0].key.as_ref(), "traceparent");
    assert_eq!(headers[0].value.as_deref(), Some(&b"first"[..]));
    assert_eq!(headers[1].key.as_ref(), "traceparent");
    assert!(headers[1].value.is_none());
    assert_eq!(headers[2].key.as_ref(), "traceparent");
    assert_eq!(headers[2].value.as_deref(), Some(&b""[..]));
}

#[test]
fn generated_wire_encoder_accepts_the_name_based_flexible_request() {
    let materialized = materialize_explicit_produce(input(usize::MAX)).unwrap();
    let version = PRODUCE_API_DESCRIPTOR.flexible_versions.unwrap().min();
    let mut frame = BytesMut::default();
    let written = encode_request(
        &mut frame,
        42,
        None,
        materialized.request(),
        version,
        OutboundFrameLimits::new(usize::MAX),
    )
    .unwrap();

    assert_eq!(written, frame.len());
    assert!(written > materialized.retained_record_bytes());
}

#[test]
fn wire_record_limits_reject_materialization_before_a_request_exists() {
    let reference = materialize_explicit_produce(input(usize::MAX)).unwrap();
    let one_byte_short = reference.retained_record_bytes() - 1;
    let error = materialize_explicit_produce(input(one_byte_short)).unwrap_err();

    assert!(matches!(
        error.record_error(),
        RecordError::BatchLimitExceeded {
            limit,
            ..
        } if *limit == one_byte_short
    ));
}

fn input(max_batch_bytes: usize) -> ExplicitProduce {
    ExplicitProduce::new(
        TOPIC.to_owned(),
        PARTITION,
        TIMESTAMP_MS,
        Some(Bytes::from_static(b"customer-42")),
        Some(Bytes::from_static(b"created")),
        REMAINING_BROKER_TIMEOUT_MS,
        max_batch_bytes,
    )
}

fn payload_input(key: Option<Bytes>, value: Option<Bytes>) -> ExplicitProduce {
    ExplicitProduce::new(
        TOPIC.to_owned(),
        PARTITION,
        TIMESTAMP_MS,
        key,
        value,
        REMAINING_BROKER_TIMEOUT_MS,
        usize::MAX,
    )
}

fn decoded_batch(materialized: &MaterializedProduce) -> RecordBatch {
    let mut encoded = materialized.request().topic_data[0].partition_data[0]
        .records
        .clone()
        .unwrap();
    RecordBatch::decode(&mut encoded, RecordDecodeLimits::default()).unwrap()
}
