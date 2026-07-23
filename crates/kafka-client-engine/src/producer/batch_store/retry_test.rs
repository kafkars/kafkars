//! Retry-wait phase and exact execution-generation scenarios.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex, PayloadId,
    TopicId,
};

use super::{BatchCancellationPhase, BatchRoute, BatchStore};
use crate::producer::ProducerStoreError;

#[test]
fn definitely_unsent_attempt_waits_before_rematerialization() {
    for submitted in [false, true] {
        let previous = execution(1);
        let replacement = execution(2);
        let mut batches = materialized_batch(previous);
        if submitted {
            let plan = batches
                .plan_driver_accepted(previous)
                .unwrap_or_else(|error| panic!("driver acceptance preflight: {error}"));
            batches.commit_driver_accepted(plan);
        }

        batches
            .start_retry(previous, replacement)
            .unwrap_or_else(|error| panic!("start retry: {error}"));
        assert_eq!(
            batches.cancellation_phase(OperationId::from_raw(2)),
            Ok(Some(BatchCancellationPhase::RetryWaiting(replacement)))
        );

        batches
            .activate_retry(replacement)
            .unwrap_or_else(|error| panic!("activate retry: {error}"));
        assert_eq!(
            batches.cancellation_phase(OperationId::from_raw(2)),
            Ok(Some(BatchCancellationPhase::Sealed(replacement)))
        );
    }
}

#[test]
fn retry_rejects_skipped_or_stale_generations_without_mutation() {
    let previous = execution(1);
    let skipped = execution(3);
    let mut batches = materialized_batch(previous);

    assert_eq!(
        batches.start_retry(previous, skipped),
        Err(ProducerStoreError::StaleBatchExecution)
    );
    assert_eq!(batches.execution(BatchId::from_raw(1)), Ok(Some(previous)));
}

fn materialized_batch(execution: BatchExecutionId) -> BatchStore {
    let mut batches = BatchStore::new(1);
    batches
        .append(
            execution.batch_id(),
            OperationId::from_raw(2),
            PayloadId::from_raw(3),
            BatchRoute {
                topic_id: TopicId::from_raw(4),
                partition: PartitionIndex::from_raw(5),
            },
        )
        .unwrap_or_else(|error| panic!("append: {error}"));
    batches
        .seal_for_materialization(execution)
        .unwrap_or_else(|error| panic!("seal: {error}"));
    let (attempt, _plan) = batches
        .begin_materialization(execution)
        .unwrap_or_else(|error| panic!("begin: {error}"));
    batches
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("commit: {error}"));
    batches
}

fn execution(generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(1),
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("nonzero generation")),
    )
}
