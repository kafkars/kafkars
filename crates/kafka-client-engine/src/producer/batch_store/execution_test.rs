//! Execution-phase tests for exact generation ownership in the batch store.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex, PayloadId,
    TopicId,
};

use super::{BatchRoute, BatchState, BatchStore, MaterializationAbort};
use crate::producer::ProducerStoreError;

fn execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

fn next(current: BatchExecutionId) -> BatchExecutionId {
    BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second generation must be valid")),
    )
}

#[test]
fn sealed_batch_execution_rejects_stale_phase_changes() {
    let batch_id = BatchId::from_raw(1);
    let current = execution(batch_id);
    let stale = next(current);
    let mut batches = BatchStore::new(1);
    batches
        .append(
            batch_id,
            OperationId::from_raw(2),
            PayloadId::from_raw(3),
            BatchRoute {
                topic_id: TopicId::from_raw(4),
                partition: PartitionIndex::from_raw(5),
            },
        )
        .unwrap_or_else(|error| panic!("append failed: {error}"));

    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    assert!(matches!(
        batches.begin_materialization(stale),
        Err(ProducerStoreError::StaleBatchExecution)
    ));
    let (attempt, _plan) = batches
        .begin_materialization(current)
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    batches
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("exact finish failed: {error}"));
    assert_eq!(
        batches.execution_route(stale),
        Err(ProducerStoreError::StaleBatchExecution)
    );
    assert!(batches.execution_route(current).is_ok());
}

#[test]
fn stale_abort_does_not_revoke_replacement_execution() {
    let batch_id = BatchId::from_raw(6);
    let current = execution(batch_id);
    let replacement = next(current);
    let mut batches = one_member_batch(batch_id);
    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let (attempt, _plan) = batches
        .begin_materialization(current)
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    let batch = batches
        .batches
        .get_mut(&batch_id)
        .unwrap_or_else(|| panic!("batch missing"));
    batch.state = BatchState::ReadyForMaterialization(replacement);

    assert_eq!(
        batches.abort_materialization(attempt),
        MaterializationAbort::Superseded
    );
    assert!(batches.begin_materialization(replacement).is_ok());
}

#[test]
fn aborted_materialization_keeps_sealed_membership_closed() {
    let batch_id = BatchId::from_raw(8);
    let operation_id = OperationId::from_raw(9);
    let current = execution(batch_id);
    let mut batches = BatchStore::new(1);
    batches
        .append(
            batch_id,
            operation_id,
            PayloadId::from_raw(10),
            BatchRoute {
                topic_id: TopicId::from_raw(11),
                partition: PartitionIndex::from_raw(12),
            },
        )
        .unwrap_or_else(|error| panic!("append failed: {error}"));
    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let (attempt, _plan) = batches
        .begin_materialization(current)
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    assert_eq!(
        batches.abort_materialization(attempt),
        MaterializationAbort::Restored
    );

    assert_eq!(
        batches.append(
            batch_id,
            OperationId::from_raw(13),
            PayloadId::from_raw(14),
            BatchRoute {
                topic_id: TopicId::from_raw(11),
                partition: PartitionIndex::from_raw(12),
            },
        ),
        Err(ProducerStoreError::BatchAlreadyMaterialized)
    );
    assert_eq!(
        batches.remove_member(batch_id, operation_id),
        Err(ProducerStoreError::BatchAlreadyMaterialized)
    );
    let (attempt, _plan) = batches
        .begin_materialization(current)
        .unwrap_or_else(|error| panic!("exact replay failed: {error}"));
    batches
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("finish failed: {error}"));
}

fn one_member_batch(batch_id: BatchId) -> BatchStore {
    let mut batches = BatchStore::new(1);
    batches
        .append(
            batch_id,
            OperationId::from_raw(20),
            PayloadId::from_raw(21),
            BatchRoute {
                topic_id: TopicId::from_raw(22),
                partition: PartitionIndex::from_raw(23),
            },
        )
        .unwrap_or_else(|error| panic!("append failed: {error}"));
    batches
}
