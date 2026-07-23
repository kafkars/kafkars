//! Ordered sealed-membership revision and all-or-nothing scenarios.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex, PayloadId,
    TopicId,
};

use super::{
    BatchCancellationPhase, BatchRevisionExpectation, BatchRevisionReplacement, BatchRoute,
    BatchStore,
};
use crate::producer::ProducerStoreError;

#[test]
fn revision_preserves_survivor_order_and_advances_exactly_once() {
    let batch_id = BatchId::from_raw(1);
    let current = execution(batch_id, 1);
    let replacement = execution(batch_id, 2);
    let mut batches = batch(batch_id, &[10, 11, 12]);
    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let plan = batches
        .plan_revision(
            current,
            OperationId::from_raw(11),
            BatchRevisionExpectation::ReadyForMaterialization,
        )
        .unwrap_or_else(|error| panic!("revision preflight failed: {error}"));
    assert_eq!(
        plan.expected_replacement(),
        BatchRevisionReplacement::Next(replacement)
    );

    batches.commit_revision(plan);

    let retained = batches
        .batches
        .get(&batch_id)
        .unwrap_or_else(|| panic!("replacement batch missing"));
    assert_eq!(
        retained
            .members
            .iter()
            .map(|member| member.operation_id.get())
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert_eq!(
        batches.cancellation_phase(OperationId::from_raw(10)),
        Ok(Some(BatchCancellationPhase::Sealed(replacement)))
    );
    assert_eq!(
        batches.cancellation_phase(OperationId::from_raw(11)),
        Ok(None)
    );
}

#[test]
fn final_member_revision_removes_every_membership_index() {
    let batch_id = BatchId::from_raw(2);
    let current = execution(batch_id, 1);
    let mut batches = batch(batch_id, &[20]);
    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let plan = batches
        .plan_revision(
            current,
            OperationId::from_raw(20),
            BatchRevisionExpectation::ReadyForMaterialization,
        )
        .unwrap_or_else(|error| panic!("revision preflight failed: {error}"));
    assert_eq!(plan.expected_replacement(), BatchRevisionReplacement::Empty);

    batches.commit_revision(plan);

    assert_eq!(batches.len(), 0);
    assert!(batches.operations.is_empty());
    assert!(batches.payloads.is_empty());
}

#[test]
fn stale_phase_preflight_does_not_mutate_any_index() {
    let batch_id = BatchId::from_raw(3);
    let current = execution(batch_id, 1);
    let mut batches = batch(batch_id, &[30, 31]);
    batches
        .seal_for_materialization(current)
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    let before_operations = batches.operations.clone();
    let before_payloads = batches.payloads.clone();

    assert!(matches!(
        batches.plan_revision(
            current,
            OperationId::from_raw(30),
            BatchRevisionExpectation::Materialized,
        ),
        Err(ProducerStoreError::StaleBatchExecution)
    ));
    assert_eq!(batches.operations, before_operations);
    assert_eq!(batches.payloads, before_payloads);
    assert_eq!(batches.execution(batch_id), Ok(Some(current)));
}

#[test]
fn maximum_generation_reports_exhaustion_without_mutation() {
    let batch_id = BatchId::from_raw(4);
    let maximum = execution(batch_id, u64::MAX);
    let mut batches = batch(batch_id, &[40, 41]);
    batches
        .seal_for_materialization(execution(batch_id, 1))
        .unwrap_or_else(|error| panic!("seal failed: {error}"));
    batches.replace_ready_for_test(batch_id, maximum);
    let before_operations = batches.operations.clone();
    let before_payloads = batches.payloads.clone();
    let plan = batches
        .plan_revision(
            maximum,
            OperationId::from_raw(40),
            BatchRevisionExpectation::ReadyForMaterialization,
        )
        .unwrap_or_else(|error| panic!("exhaustion preflight failed: {error}"));

    assert_eq!(
        plan.expected_replacement(),
        BatchRevisionReplacement::Exhausted
    );
    assert_eq!(batches.operations, before_operations);
    assert_eq!(batches.payloads, before_payloads);
    assert_eq!(batches.execution(batch_id), Ok(Some(maximum)));
}

fn batch(batch_id: BatchId, operations: &[u64]) -> BatchStore {
    let mut batches = BatchStore::new(1);
    for operation in operations {
        batches
            .append(
                batch_id,
                OperationId::from_raw(*operation),
                PayloadId::from_raw(*operation + 100),
                BatchRoute {
                    topic_id: TopicId::from_raw(4),
                    partition: PartitionIndex::from_raw(5),
                },
            )
            .unwrap_or_else(|error| panic!("append failed: {error}"));
    }
    batches
}

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        batch_id,
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("generation must be nonzero")),
    )
}
