//! Semantic record-batch rejection after authoritative wire decoding.

use bytes::Bytes;
use kafka_wire::FetchResponse;
use kafka_wire_records::{Compression, Record, RecordBatch, TimestampType};

use super::{
    batch_model::normalize_batch,
    failure::FetchDecodeFailure,
    limits::{FetchBudget, FetchDecodeLimits},
};

#[test]
fn negative_last_delta_and_out_of_range_records_are_rejected() {
    let mut first_budget = test_budget();
    let mut negative_delta = batch();
    negative_delta.last_offset_delta = -1;
    assert_eq!(
        normalize_batch(negative_delta, &mut first_budget),
        Err(FetchDecodeFailure::NegativeLastOffsetDelta { actual: -1 })
    );

    let mut second_budget = test_budget();
    let mut outside = batch();
    outside.records[0].offset_delta = 2;
    assert!(matches!(
        normalize_batch(outside, &mut second_budget),
        Err(FetchDecodeFailure::RecordOffsetOutsideBatch {
            offset: 12,
            first: 10,
            last: 10,
        })
    ));
}

#[test]
fn negative_log_offsets_are_rejected_before_delivery() {
    let mut budget = test_budget();
    let mut negative = batch();
    negative.base_offset = -1;
    assert_eq!(
        normalize_batch(negative, &mut budget),
        Err(FetchDecodeFailure::NegativeBaseOffset { actual: -1 })
    );
}

#[test]
fn absolute_timestamp_overflow_is_not_saturated() {
    let mut budget = test_budget();
    let mut batch = batch();
    batch.base_timestamp = i64::MAX;
    batch.max_timestamp = i64::MAX;
    batch.records[0].timestamp_delta = 1;
    assert_eq!(
        normalize_batch(batch, &mut budget),
        Err(FetchDecodeFailure::TimestampOverflow)
    );
}

#[test]
fn log_append_records_use_batch_max_not_base_plus_delta() {
    let mut budget = test_budget();
    let mut batch = batch();
    batch.timestamp_type = TimestampType::LogAppendTime;
    batch.base_timestamp = 20;
    batch.max_timestamp = 99;
    batch.records[0].timestamp_delta = 7;

    let normalized = normalize_batch(batch, &mut budget)
        .unwrap_or_else(|error| panic!("valid log-append batch: {error:?}"));
    assert_eq!(normalized.max_timestamp, Some(99));
    assert_eq!(normalized.records[0].timestamp, Some(99));
}

#[test]
fn timestamps_accept_only_the_exact_absent_sentinel() {
    let mut budget = test_budget();
    let mut absent = batch();
    absent.base_timestamp = -1;
    absent.max_timestamp = -1;
    absent.records.clear();
    let normalized = normalize_batch(absent, &mut budget)
        .unwrap_or_else(|error| panic!("exact timestamp sentinel: {error:?}"));
    assert_eq!(normalized.max_timestamp, None);
    assert!(normalized.records.is_empty());

    let mut budget = test_budget();
    let mut nonempty_absent = batch();
    nonempty_absent.base_timestamp = -1;
    nonempty_absent.max_timestamp = -1;
    assert_eq!(
        normalize_batch(nonempty_absent, &mut budget),
        Err(FetchDecodeFailure::InvalidBatchTimestamps {
            base_timestamp: -1,
            max_timestamp: -1,
        })
    );

    for (base_timestamp, max_timestamp) in [(-2, -2), (-1, 20), (20, -1)] {
        let mut budget = test_budget();
        let mut malformed = batch();
        malformed.base_timestamp = base_timestamp;
        malformed.max_timestamp = max_timestamp;
        assert_eq!(
            normalize_batch(malformed, &mut budget),
            Err(FetchDecodeFailure::InvalidBatchTimestamps {
                base_timestamp,
                max_timestamp,
            })
        );
    }
}

#[test]
fn empty_compacted_batch_retains_max_timestamp_without_record_time() {
    let mut budget = test_budget();
    let mut compacted = batch();
    compacted.base_timestamp = -1;
    compacted.max_timestamp = 20;
    compacted.records.clear();
    let normalized = normalize_batch(compacted, &mut budget)
        .unwrap_or_else(|error| panic!("empty compacted batch: {error:?}"));
    assert_eq!(normalized.max_timestamp, Some(20));
    assert!(normalized.records.is_empty());
    assert_eq!(normalized.delete_horizon_ms, None);
}

#[test]
fn leader_epoch_next_offset_and_delete_horizon_are_lossless() {
    let mut budget = test_budget();
    let ordinary = normalize_batch(batch(), &mut budget)
        .unwrap_or_else(|error| panic!("ordinary batch facts: {error:?}"));
    assert_eq!(ordinary.delete_horizon_ms, None);

    let mut budget = test_budget();
    let mut retained = batch();
    retained.partition_leader_epoch = 3;
    retained.has_delete_horizon = true;
    retained.base_timestamp = 100;
    retained.max_timestamp = 20;
    retained.records[0].timestamp_delta = -80;
    let normalized = normalize_batch(retained, &mut budget)
        .unwrap_or_else(|error| panic!("valid batch facts: {error:?}"));
    assert_eq!(normalized.partition_leader_epoch, Some(3));
    assert_eq!(normalized.next_offset, 11);
    assert_eq!(normalized.delete_horizon_ms, Some(100));
    assert_eq!(normalized.max_timestamp, Some(20));
    assert_eq!(normalized.records[0].timestamp, Some(20));

    let mut budget = test_budget();
    let mut missing_horizon = batch();
    missing_horizon.has_delete_horizon = true;
    missing_horizon.base_timestamp = -1;
    missing_horizon.max_timestamp = -1;
    assert_eq!(
        normalize_batch(missing_horizon, &mut budget),
        Err(FetchDecodeFailure::InvalidBatchTimestamps {
            base_timestamp: -1,
            max_timestamp: -1,
        })
    );

    let mut budget = test_budget();
    let mut malformed_epoch = batch();
    malformed_epoch.partition_leader_epoch = -2;
    assert_eq!(
        normalize_batch(malformed_epoch, &mut budget),
        Err(FetchDecodeFailure::InvalidPartitionLeaderEpoch { actual: -2 })
    );

    let mut budget = test_budget();
    let mut overflow = batch();
    overflow.base_offset = i64::MAX;
    assert_eq!(
        normalize_batch(overflow, &mut budget),
        Err(FetchDecodeFailure::NextOffsetOverflow {
            last_offset: i64::MAX,
        })
    );
}

pub(super) fn test_budget() -> FetchBudget {
    FetchBudget::start(&FetchResponse::default(), FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"))
}

pub(super) fn batch() -> RecordBatch {
    RecordBatch {
        base_offset: 10,
        last_offset_delta: 0,
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
            value: Some(Bytes::from_static(b"value")),
            headers: Vec::new(),
        }],
    }
}
