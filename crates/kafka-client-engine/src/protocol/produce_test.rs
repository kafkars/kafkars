//! Generated Produce request assembly stays separate from batch record semantics.

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use kafka_client_core::{Deadline, Moment};
use kafka_wire::{OutboundFrameLimits, PRODUCE_API_DESCRIPTOR, encode_request};
use kafka_wire_records::RecordError;

use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
};

use super::{error::ProduceMaterializationError, produce::materialize_explicit_produce_batch};

const TOPIC: &str = "orders";
const PARTITION: i32 = 7;
const TIMESTAMP_MS: i64 = 1_725_000_000_123;

#[test]
fn explicit_partition_batch_becomes_one_acks_all_generated_request() {
    let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
    let request = materialized.into_name_routed_request(
        Moment::from_tick(10),
        operation_deadline(30_000_000_010, Instant::now()),
    );

    assert!(request.transactional_id.is_none());
    assert_eq!(request.acks, -1);
    assert_eq!(request.timeout_ms, 30_000);
    assert_eq!(request.topic_data.len(), 1);
    assert_eq!(request.topic_data[0].name.as_ref(), TOPIC);
    assert_eq!(request.topic_data[0].partition_data.len(), 1);
    assert_eq!(request.topic_data[0].partition_data[0].index, PARTITION);
}

#[test]
fn generated_wire_encoder_accepts_the_name_based_flexible_request() {
    let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
    let retained_record_bytes = materialized.retained_record_bytes();
    let request = materialized.into_name_routed_request(
        Moment::from_tick(10),
        operation_deadline(15_000_000_010, Instant::now()),
    );
    let version = PRODUCE_API_DESCRIPTOR.flexible_versions.unwrap().min();
    let mut frame = BytesMut::default();
    let written = encode_request(
        &mut frame,
        42,
        None,
        &request,
        version,
        OutboundFrameLimits::new(usize::MAX),
    )
    .unwrap();

    assert_eq!(written, frame.len());
    assert!(written > retained_record_bytes);
}

#[test]
fn submission_binds_timeout_without_reencoding_record_batch_bytes() {
    for timeout_ms in [1_000, 30_000] {
        let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
        assert_eq!(materialized.topic_name(), TOPIC);
        assert_eq!(materialized.partition(), PARTITION);
        let encoded_address = materialized.encoded_records().as_ptr();
        let encoded_length = materialized.encoded_records().len();
        let now = Moment::from_tick(10);
        let deadline_tick = now
            .tick()
            .checked_add(u64::try_from(timeout_ms).unwrap() * 1_000_000)
            .unwrap();
        let request = materialized
            .into_name_routed_request(now, operation_deadline(deadline_tick, Instant::now()));
        let records = request.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .unwrap();

        assert_eq!(request.timeout_ms, timeout_ms);
        assert_eq!(records.as_ptr(), encoded_address);
        assert_eq!(records.len(), encoded_length);
    }
}

#[test]
fn broker_timeout_ceil_rounds_and_clamps_the_original_absolute_deadline() {
    let cases = [
        (10, 10, 0),
        (10, 11, 1),
        (10, 1_000_010, 1),
        (10, 1_000_011, 2),
        (0, u64::MAX, i32::MAX),
    ];

    for (now_tick, deadline_tick, expected_timeout_ms) in cases {
        let materialized = materialize_explicit_produce_batch(input(usize::MAX)).unwrap();
        let transport = Instant::now();
        let deadline = operation_deadline(deadline_tick, transport);
        let request = materialized.into_name_routed_request(Moment::from_tick(now_tick), deadline);

        assert_eq!(request.timeout_ms, expected_timeout_ms);
        assert_eq!(deadline.transport(), transport);
    }
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
    let input = MaterializationBatch::new(TOPIC.to_owned(), PARTITION, Vec::new(), usize::MAX);

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
        max_batch_bytes,
    )
}

fn operation_deadline(tick: u64, transport: Instant) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), transport)
}
