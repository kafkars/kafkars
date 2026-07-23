//! Exact materialized-to-submitted engine phase scenarios.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex, PayloadId,
    TopicId,
};

use super::{BatchCancellationPhase, BatchRoute, BatchStore};
use crate::producer::ProducerStoreError;

#[test]
fn exact_driver_acceptance_marks_the_engine_batch_submitted() {
    let batch_id = BatchId::from_raw(1);
    let operation_id = OperationId::from_raw(2);
    let execution = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());
    let mut batches = materialized(batch_id, operation_id, execution);
    let plan = batches
        .plan_driver_accepted(execution)
        .unwrap_or_else(|error| panic!("driver preflight failed: {error}"));

    batches.commit_driver_accepted(plan);

    assert_eq!(
        batches.cancellation_phase(operation_id),
        Ok(Some(BatchCancellationPhase::Submitted))
    );
    assert_eq!(batches.execution(batch_id), Ok(Some(execution)));
    assert!(matches!(
        batches.plan_driver_accepted(execution),
        Err(ProducerStoreError::StaleBatchExecution)
    ));
}

fn materialized(
    batch_id: BatchId,
    operation_id: OperationId,
    execution: BatchExecutionId,
) -> BatchStore {
    let mut batches = BatchStore::new(1);
    batches
        .append(
            batch_id,
            operation_id,
            PayloadId::from_raw(3),
            BatchRoute {
                topic_id: TopicId::from_raw(4),
                partition: PartitionIndex::from_raw(5),
            },
        )
        .unwrap_or_else(|error| panic!("append failed: {error}"));
    batches
        .seal_for_materialization(execution)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let (attempt, _plan) = batches
        .begin_materialization(execution)
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    batches
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));
    batches
}
