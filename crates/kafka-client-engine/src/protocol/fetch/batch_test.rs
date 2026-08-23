//! Ordered multi-batch offset fencing after authoritative wire decoding.

use bytes::{Bytes, BytesMut};
use kafka_wire::FetchResponse;
use kafka_wire_records::{Compression, Record, RecordBatch, RecordEncodeLimits, TimestampType};

use super::{
    batch::{decode_batches, decode_record_payload},
    failure::FetchDecodeFailure,
    limits::{FetchBudget, FetchDecodeLimits},
};

#[test]
fn batches_must_be_monotonic_and_non_overlapping() {
    let encoded = encoded_batches(&[batch(10, 4), batch(14, 0)]);
    let mut budget = test_budget();
    assert_eq!(
        decode_batches(encoded, 2, 3, &mut budget),
        Err(FetchDecodeFailure::BatchOffsetOverlap {
            previous_last_offset: 14,
            base_offset: 14,
        })
    );
}

#[test]
fn separated_batches_preserve_their_next_offsets() {
    let encoded = encoded_batches(&[batch(10, 2), batch(20, 0)]);
    let mut budget = test_budget();
    let batches = decode_batches(encoded, 0, 0, &mut budget)
        .unwrap_or_else(|error| panic!("nonoverlapping batches: {error:?}"));
    assert_eq!(batches[0].next_offset, 13);
    assert_eq!(batches[1].next_offset, 21);
}

#[test]
fn standalone_payload_decode_retains_exact_record_and_logical_byte_counts() {
    let mut first = batch(10, 1);
    first.records = vec![
        Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: Vec::new(),
        },
        Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 1,
            key: None,
            value: Some(Bytes::from_static(b"second")),
            headers: Vec::new(),
        },
    ];
    let payload = decode_record_payload(encoded_batches(&[first]), FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("standalone payload: {error:?}"));
    assert_eq!(payload.records, 2);
    assert_eq!(payload.logical_bytes, 14);
    assert!(payload.retained_bytes >= payload.logical_bytes);
    assert_eq!(payload.batches[0].records[1].offset, 11);
}

fn test_budget() -> FetchBudget {
    FetchBudget::start(&FetchResponse::default(), FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"))
}

fn encoded_batches(batches: &[RecordBatch]) -> Bytes {
    let mut encoded = BytesMut::new();
    for batch in batches {
        batch
            .encode_into(&mut encoded, RecordEncodeLimits::default())
            .unwrap_or_else(|error| panic!("test batch encoding: {error}"));
    }
    encoded.freeze()
}

fn batch(base_offset: i64, last_offset_delta: i32) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta,
        partition_leader_epoch: -1,
        compression: Compression::None,
        timestamp_type: TimestampType::CreateTime,
        is_transactional: false,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp: 20,
        max_timestamp: 20,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: None,
            headers: Vec::new(),
        }],
    }
}
