//! Semantic record-batch rejection after authoritative wire decoding.

use bytes::Bytes;
use kafka_wire::FetchResponse;
use kafka_wire_records::{Compression, Record, RecordBatch, TimestampType};

use super::{
    batch::normalize_batch,
    failure::FetchDecodeFailure,
    limits::{FetchBudget, FetchDecodeLimits},
};

#[test]
fn negative_last_delta_and_out_of_range_records_are_rejected() {
    let mut first_budget = budget();
    let mut negative_delta = batch();
    negative_delta.last_offset_delta = -1;
    assert_eq!(
        normalize_batch(negative_delta, &mut first_budget),
        Err(FetchDecodeFailure::NegativeLastOffsetDelta { actual: -1 })
    );

    let mut second_budget = budget();
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
    let mut budget = budget();
    let mut negative = batch();
    negative.base_offset = -1;
    assert_eq!(
        normalize_batch(negative, &mut budget),
        Err(FetchDecodeFailure::NegativeBaseOffset { actual: -1 })
    );
}

#[test]
fn absolute_timestamp_overflow_is_not_saturated() {
    let mut budget = budget();
    let mut batch = batch();
    batch.base_timestamp = i64::MAX;
    batch.records[0].timestamp_delta = 1;
    assert_eq!(
        normalize_batch(batch, &mut budget),
        Err(FetchDecodeFailure::TimestampOverflow)
    );
}

fn budget() -> FetchBudget {
    FetchBudget::start(&FetchResponse::default(), FetchDecodeLimits::default())
        .unwrap_or_else(|error| panic!("empty response budget: {error:?}"))
}

fn batch() -> RecordBatch {
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
