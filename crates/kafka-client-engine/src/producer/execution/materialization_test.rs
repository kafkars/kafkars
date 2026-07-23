//! Materialization failure rollback and exact stale-commit scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, CompressionPolicy, Moment, OperationId,
    PartitionIndex,
};

use super::{PreparedExecution, PreparedExecutionError, PreparedExecutionLimits};
use crate::{
    producer::{ProducerRecord, ProducerStore, ProducerStoreLimits},
    protocol::produce::materialize_explicit_produce_batch,
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

fn prepared(max_batch_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes,
        },
    )
}

#[test]
fn stale_commit_drops_the_exact_prepared_bytes_it_inserted() {
    let batch_id = BatchId::from_raw(1);
    let operation_id = OperationId::from_raw(2);
    let current = execution(batch_id, 1);
    let replacement = execution(batch_id, 2);
    let mut store = store(batch_id, operation_id);
    let (attempt, input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("view failed: {error}"));
    let encoded = materialize_explicit_produce_batch(input)
        .unwrap_or_else(|error| panic!("encoding failed: {error}"));
    let mut prepared = prepared(1_024);
    prepared
        .prepared
        .insert(current, encoded)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    store.replace_batch_execution_for_test(batch_id, replacement);

    assert_eq!(
        prepared.commit_inserted(&mut store, attempt, Moment::from_tick(3)),
        Err(PreparedExecutionError::Store(
            crate::producer::ProducerStoreError::StaleBatchExecution
        ))
    );
    assert_eq!(prepared.prepared_stats().batches, 0);
    assert_eq!(prepared.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(store.batch_execution(batch_id), Ok(Some(replacement)));
}

#[test]
fn semantic_encoding_failure_returns_exact_attempt_to_ready() {
    let batch_id = BatchId::from_raw(6);
    let operation_id = OperationId::from_raw(7);
    let current = execution(batch_id, 1);
    let mut store = store(batch_id, operation_id);
    let mut prepared = prepared(1);

    assert_eq!(
        prepared.materialize(
            &mut store,
            current,
            CompressionPolicy::Uncompressed,
            Moment::from_tick(8),
        ),
        Ok(kafka_client_core::ProducerInput::BatchMaterializationFailed { execution: current })
    );
    let (retry, _input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("ready retry failed: {error}"));
    assert!(matches!(
        store.abort_materialization(retry),
        crate::producer::batch_store::MaterializationAbort::Restored
    ));
}

#[test]
fn prepared_capacity_failure_returns_exact_attempt_to_ready() {
    let batch_id = BatchId::from_raw(9);
    let operation_id = OperationId::from_raw(10);
    let current = execution(batch_id, 1);
    let mut store = store(batch_id, operation_id);
    let mut prepared = PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1,
            max_batch_bytes: 1_024,
        },
    );

    assert_eq!(
        prepared.materialize(
            &mut store,
            current,
            CompressionPolicy::Uncompressed,
            Moment::from_tick(11),
        ),
        Ok(kafka_client_core::ProducerInput::BatchMaterializationFailed { execution: current })
    );
    assert_eq!(prepared.prepared_stats().batches, 0);
    let (retry, _input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("ready retry failed: {error}"));
    assert!(matches!(
        store.abort_materialization(retry),
        crate::producer::batch_store::MaterializationAbort::Restored
    ));
}
