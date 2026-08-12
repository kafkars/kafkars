//! Terminal cleanup preflight and outside-in recovery scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId, PartitionIndex,
};

use super::{PreparedExecution, PreparedExecutionError, PreparedExecutionLimits};
use crate::protocol::produce::materialize_explicit_produce_batch;
use crate::{
    clock::OperationDeadline,
    producer::{ProducerRecord, ProducerStore, ProducerStoreLimits},
};

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        batch_id,
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("test generation must be nonzero")),
    )
}

fn store(batch_id: BatchId, operation_id: OperationId) -> ProducerStore {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(3),
            10,
            None,
            Some(Bytes::from_static(b"value")),
        ))
        .unwrap_or_else(|error| panic!("reservation failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    store
}

fn prepared() -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    )
}

#[test]
fn cleanup_preflight_preserves_wrong_generation_until_outside_in_recovery() {
    let batch_id = BatchId::from_raw(4);
    let operation_id = OperationId::from_raw(5);
    let current = execution(batch_id, 1);
    let replacement = execution(batch_id, 2);
    let mut store = store(batch_id, operation_id);
    let (attempt, input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("view failed: {error}"));
    store
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("materialization commit failed: {error}"));
    let encoded = materialize_explicit_produce_batch(input)
        .unwrap_or_else(|error| panic!("encoding failed: {error}"));
    let mut prepared = prepared();
    prepared
        .retain_for_test(replacement, encoded)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    prepared
        .arm_for_test(
            replacement,
            operation_id,
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));

    assert!(matches!(
        prepared.release_batch(&mut store, batch_id),
        Err(PreparedExecutionError::CleanupExecutionMismatch {
            expected: Some(actual),
            retained: Some(retained),
            ..
        }) if actual == current
            && retained == replacement
    ));
    assert_eq!(store.stats().batches, 1);
    assert_eq!(prepared.prepared_stats().batches, 1);
    assert_eq!(prepared.submission_count(), 1);

    prepared.clear_terminal();
    store.clear_terminal();
    assert_eq!(store.stats().records, 0);
    assert_eq!(store.stats().batches, 0);
    assert_eq!(prepared.prepared_stats().batches, 0);
    assert_eq!(prepared.submission_count(), 0);
}
