//! Ordered multi-record batches preserve application fields and Kafka deltas.

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    CompressionPolicy, Deadline, Moment, TransactionSequenceLease, TransactionalProducerIdentity,
};
use kafka_wire_records::{Compression, RecordBatch, RecordDecodeLimits, TimestampType};

use crate::{
    clock::OperationDeadline,
    producer::materialization::{
        MaterializationBatch, MaterializationHeader, MaterializationRecord,
        TransactionalMaterializationBatch,
    },
    protocol::error::ProduceMaterializationError,
};

use super::{
    MaterializedProduce, materialize_explicit_produce_batch,
    materialize_explicit_produce_batch_with_compression, materialize_transactional_produce_batch,
};

const TOPIC: &str = "orders";
const PARTITION: i32 = 7;
const BASE_TIMESTAMP_MS: i64 = 1_000;

#[test]
fn ordered_records_encode_identity_sequence_and_contiguous_offsets() {
    let materialized = materialize_explicit_produce_batch(batch(vec![
        record(BASE_TIMESTAMP_MS, b"first"),
        record(1_600, b"second"),
        record(900, b"third"),
    ]))
    .unwrap();
    let decoded = decoded_batch(&materialized);

    assert_eq!(decoded.compression, Compression::None);
    assert_eq!(decoded.timestamp_type, TimestampType::CreateTime);
    assert_eq!(decoded.base_timestamp, BASE_TIMESTAMP_MS);
    assert_eq!(decoded.max_timestamp, 1_600);
    assert_eq!(decoded.last_offset_delta, 2);
    assert_eq!(decoded.producer_id, 1);
    assert_eq!(decoded.producer_epoch, 0);
    assert_eq!(decoded.base_sequence, 0);
    assert_eq!(decoded.records.len(), 3);
    assert_eq!(decoded.records[0].offset_delta, 0);
    assert_eq!(decoded.records[1].offset_delta, 1);
    assert_eq!(decoded.records[2].offset_delta, 2);
    assert_eq!(decoded.records[0].timestamp_delta, 0);
    assert_eq!(decoded.records[1].timestamp_delta, 600);
    assert_eq!(decoded.records[2].timestamp_delta, -100);
    assert_eq!(decoded.records[0].value.as_deref(), Some(&b"first"[..]));
    assert_eq!(decoded.records[1].value.as_deref(), Some(&b"second"[..]));
    assert_eq!(decoded.records[2].value.as_deref(), Some(&b"third"[..]));
}

#[test]
fn transaction_batch_and_generated_request_retain_the_same_fenced_identity() {
    let identity = TransactionalProducerIdentity::try_new(41, 3)
        .unwrap_or_else(|| panic!("transaction identity must be valid"));
    let sequence = TransactionSequenceLease::try_new(7, 1)
        .unwrap_or_else(|| panic!("transaction sequence must be valid"));
    let input = TransactionalMaterializationBatch::new(
        TOPIC,
        PARTITION,
        vec![record(BASE_TIMESTAMP_MS, b"created")],
        usize::MAX,
        identity,
        sequence,
    );
    let materialized = materialize_transactional_produce_batch(input, CompressionPolicy::Snappy)
        .unwrap_or_else(|error| panic!("transaction materialization failed: {error}"));
    let decoded = decoded_batch(&materialized);

    assert!(decoded.is_transactional);
    assert!(!decoded.is_control);
    assert_eq!(decoded.producer_id, 41);
    assert_eq!(decoded.producer_epoch, 3);
    assert_eq!(decoded.base_sequence, 7);
    assert_eq!(decoded.compression, Compression::Snappy);

    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(1), Instant::now());
    let request = materialized.transactional_name_routed_request(
        "invoice-writer",
        Moment::from_tick(0),
        deadline,
    );
    assert_eq!(
        request
            .transactional_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("invoice-writer")
    );
}

#[test]
fn null_and_empty_payloads_remain_distinct_in_one_batch() {
    let materialized = materialize_explicit_produce_batch(batch(vec![
        MaterializationRecord::new(BASE_TIMESTAMP_MS, None, None, Vec::new()),
        MaterializationRecord::new(
            BASE_TIMESTAMP_MS + 1,
            Some(Bytes::new()),
            Some(Bytes::new()),
            Vec::new(),
        ),
    ]))
    .unwrap();
    let decoded = decoded_batch(&materialized);

    assert!(decoded.records[0].key.is_none());
    assert!(decoded.records[0].value.is_none());
    assert_eq!(decoded.records[1].key.as_deref(), Some(&b""[..]));
    assert_eq!(decoded.records[1].value.as_deref(), Some(&b""[..]));
}

#[test]
fn headers_preserve_record_order_duplicates_and_nullable_values() {
    let headers = vec![
        MaterializationHeader::new("traceparent".to_owned(), Some(Bytes::from_static(b"first"))),
        MaterializationHeader::new("traceparent".to_owned(), None),
        MaterializationHeader::new("traceparent".to_owned(), Some(Bytes::new())),
    ];
    let materialized = materialize_explicit_produce_batch(batch(vec![
        record(BASE_TIMESTAMP_MS, b"first"),
        MaterializationRecord::new(
            BASE_TIMESTAMP_MS + 1,
            None,
            Some(Bytes::from_static(b"second")),
            headers,
        ),
    ]))
    .unwrap();
    let decoded = decoded_batch(&materialized);
    let headers = &decoded.records[1].headers;

    assert!(decoded.records[0].headers.is_empty());
    assert_eq!(headers.len(), 3);
    assert_eq!(headers[0].key.as_ref(), "traceparent");
    assert_eq!(headers[0].value.as_deref(), Some(&b"first"[..]));
    assert_eq!(headers[1].key.as_ref(), "traceparent");
    assert!(headers[1].value.is_none());
    assert_eq!(headers[2].key.as_ref(), "traceparent");
    assert_eq!(headers[2].value.as_deref(), Some(&b""[..]));
}

#[test]
fn unrepresentable_timestamp_delta_is_rejected_without_saturation() {
    let input = batch(vec![
        record(i64::MIN, b"first"),
        record(i64::MAX, b"second"),
    ]);

    assert!(matches!(
        materialize_explicit_produce_batch(input),
        Err(ProduceMaterializationError::TimestampDeltaOverflow {
            base_timestamp_ms: i64::MIN,
            timestamp_ms: i64::MAX,
        })
    ));
}

#[test]
fn every_client_policy_uses_the_wire_records_codec_authority() {
    let pairs = [
        (
            kafka_client_core::CompressionPolicy::Gzip,
            Compression::Gzip,
        ),
        (
            kafka_client_core::CompressionPolicy::Snappy,
            Compression::Snappy,
        ),
        (kafka_client_core::CompressionPolicy::Lz4, Compression::Lz4),
        (
            kafka_client_core::CompressionPolicy::Zstd,
            Compression::Zstd,
        ),
    ];
    for (policy, expected) in pairs {
        let materialized = materialize_explicit_produce_batch_with_compression(
            batch(vec![
                record(BASE_TIMESTAMP_MS, b"compressible payload"),
                record(BASE_TIMESTAMP_MS + 1, b"compressible payload"),
            ]),
            policy,
        )
        .unwrap_or_else(|error| panic!("{expected:?} materialization failed: {error}"));
        assert_eq!(decoded_batch(&materialized).compression, expected);
    }
}

fn batch(records: Vec<MaterializationRecord>) -> MaterializationBatch {
    MaterializationBatch::try_for_test(TOPIC.to_owned(), PARTITION, records, usize::MAX)
        .unwrap_or_else(|| panic!("test materialization batch must be representable"))
}

fn record(timestamp_ms: i64, value: &'static [u8]) -> MaterializationRecord {
    MaterializationRecord::new(
        timestamp_ms,
        None,
        Some(Bytes::from_static(value)),
        Vec::new(),
    )
}

fn decoded_batch(materialized: &MaterializedProduce) -> RecordBatch {
    let mut encoded = materialized.encoded_records().clone();
    let decoded = RecordBatch::decode(&mut encoded, RecordDecodeLimits::default()).unwrap();
    assert!(encoded.is_empty());
    decoded
}
