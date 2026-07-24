//! Fetch-specific partial-tail and cumulative retained-payload scenarios.

use bytes::BytesMut;
use kafka_wire_records::{Compression, RecordError};

use super::{
    FetchDecodeFailure, FetchDecodeLimits,
    batch_model_test::batch,
    decode_test::{batch_bytes, partition, record_bytes, record_bytes_at, response, topic},
    normalize_fetch_response,
};

#[test]
fn compressed_batches_share_exact_cumulative_retained_payload_budget() {
    const RETAINED_PER_BATCH: usize = 20;
    let first = record_bytes(Compression::Gzip);
    let second = record_bytes_at(Compression::Gzip, 43);
    let mut records = BytesMut::with_capacity(first.len() + second.len());
    records.extend_from_slice(&first);
    records.extend_from_slice(&second);
    let records = records.freeze();
    let limits = FetchDecodeLimits {
        max_additional_retained_payload_bytes: RETAINED_PER_BATCH * 2,
        ..FetchDecodeLimits::default()
    };
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "compressed",
            vec![partition(0, Some(records.clone()))],
        )]),
        limits,
    )
    .unwrap_or_else(|error| panic!("exact cumulative retained payload: {error:?}"));
    assert_eq!(normalized.topics[0].partitions[0].batches.len(), 2);

    let limits = FetchDecodeLimits {
        max_additional_retained_payload_bytes: RETAINED_PER_BATCH * 2 - 1,
        ..FetchDecodeLimits::default()
    };
    assert_eq!(
        normalize_fetch_response(
            response(vec![
                topic("compressed", vec![partition(0, Some(records))],)
            ]),
            limits,
        ),
        Err(FetchDecodeFailure::RecordBatch {
            topic: 0,
            partition: 0,
            batch: 1,
            source: RecordError::RetainedPayloadLimitExceeded {
                length: RETAINED_PER_BATCH,
                limit: RETAINED_PER_BATCH - 1,
            },
        })
    );
}

#[test]
fn valid_partial_trailing_batch_is_not_reported_as_corruption() {
    let complete = record_bytes(Compression::None);
    let partial = record_bytes_at(Compression::None, 43);
    let mut records = BytesMut::with_capacity(complete.len() + 4);
    records.extend_from_slice(&complete);
    records.extend_from_slice(&partial[..4]);
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "partial",
            vec![partition(0, Some(records.freeze()))],
        )]),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("valid partial tail must be ignored: {error:?}"));
    let batches = &normalized.topics[0].partitions[0].batches;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].next_offset, 43);
}

#[test]
fn encoded_control_batch_preserves_exact_sequence_sentinel() {
    let mut control = batch();
    control.producer_id = 7;
    control.producer_epoch = 2;
    control.base_sequence = -1;
    control.is_transactional = true;
    control.is_control = true;
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "control",
            vec![partition(0, Some(batch_bytes(&control)))],
        )]),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("encoded control batch: {error:?}"));
    let batch = &normalized.topics[0].partitions[0].batches[0];
    assert!(batch.is_transactional);
    assert!(batch.is_control);
    assert_eq!(
        batch.producer.map(|identity| identity.base_sequence),
        Some(-1)
    );
}

#[test]
fn encoded_kraft_control_batch_is_identity_free() {
    let mut control = batch();
    control.is_control = true;
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "kraft-control",
            vec![partition(0, Some(batch_bytes(&control)))],
        )]),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("canonical KRaft control batch: {error:?}"));
    let batch = &normalized.topics[0].partitions[0].batches[0];
    assert!(!batch.is_transactional);
    assert!(batch.is_control);
    assert_eq!(batch.producer, None);

    control.producer_id = 7;
    control.producer_epoch = 2;
    let encoded = batch_bytes(&control);
    assert_eq!(
        normalize_fetch_response(
            response(vec![topic(
                "kraft-control",
                vec![partition(0, Some(encoded))],
            )]),
            FetchDecodeLimits::default(),
        ),
        Err(FetchDecodeFailure::NonTransactionalControlIdentity)
    );
}

#[test]
fn encoded_empty_compacted_batch_retains_only_max_timestamp() {
    let mut compacted = batch();
    compacted.base_timestamp = -1;
    compacted.max_timestamp = 20;
    compacted.records.clear();
    let normalized = normalize_fetch_response(
        response(vec![topic(
            "compacted",
            vec![partition(0, Some(batch_bytes(&compacted)))],
        )]),
        FetchDecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("encoded empty compacted batch: {error:?}"));
    let batch = &normalized.topics[0].partitions[0].batches[0];
    assert_eq!(batch.max_timestamp, Some(20));
    assert!(batch.records.is_empty());
    assert_eq!(batch.delete_horizon_ms, None);
}
