//! Generated Produce request assembly stays separate from batch record semantics.

#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use kafka_wire::{OutboundFrameLimits, PRODUCE_API_DESCRIPTOR, encode_request};
use kafka_wire_records::RecordError;

use crate::producer::{MaterializationBatch, MaterializationRecord};

use super::{error::ProduceMaterializationError, produce::materialize_explicit_produce_batch};

const TOPIC: &str = "orders";
const PARTITION: i32 = 7;
const TIMESTAMP_MS: i64 = 1_725_000_000_123;
const REMAINING_BROKER_TIMEOUT_MS: i32 = 30_000;

#[test]
fn explicit_partition_batch_becomes_one_acks_all_generated_request() {
    let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
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
fn generated_wire_encoder_accepts_the_name_based_flexible_request() {
    let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
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
    let reference = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
    let one_byte_short = reference.retained_record_bytes() - 1;
    let error = materialize_explicit_produce_batch(input(one_byte_short)).unwrap_err();

    assert!(matches!(
        error.record_error(),
        Some(RecordError::BatchLimitExceeded {
            limit,
            ..
        }) if *limit == one_byte_short
    ));
}

#[test]
fn empty_ready_batch_is_rejected_before_wire_encoding() {
    let input = MaterializationBatch::new(
        TOPIC.to_owned(),
        PARTITION,
        Vec::new(),
        REMAINING_BROKER_TIMEOUT_MS,
        usize::MAX,
    );

    assert!(matches!(
        materialize_explicit_produce_batch(input),
        Err(ProduceMaterializationError::EmptyBatch)
    ));
}

fn input(max_batch_bytes: usize) -> MaterializationBatch {
    MaterializationBatch::new(
        TOPIC.to_owned(),
        PARTITION,
        vec![MaterializationRecord::new(
            TIMESTAMP_MS,
            Some(Bytes::from_static(b"customer-42")),
            Some(Bytes::from_static(b"created")),
            Vec::new(),
        )],
        REMAINING_BROKER_TIMEOUT_MS,
        max_batch_bytes,
    )
}
