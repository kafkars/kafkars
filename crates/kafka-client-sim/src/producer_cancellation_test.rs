//! Scenario traces prove exact execution replacement and resource disposal.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, DeliveryStatus,
    ExplicitRecord, Moment, OperationId, PartitionIndex, PayloadId, ProducerBatchPolicy,
    ProducerCancellationOutcome, ProducerCompletion, ProducerEffect, ProducerFailureKind,
    ProducerInput, TopicId,
};

use crate::ProducerScenario;

const BYTES: ByteCount = ByteCount::new(8);

#[test]
fn awaiting_driver_cancellation_discards_old_submission_and_rematerializes_survivor() {
    let (mut scenario, survivor, cancelled, previous) = sealed_pair();
    scenario
        .step(ProducerInput::BatchMaterialized {
            execution: previous,
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization fact failed: {error}"));
    assert_eq!(scenario.submission_count(), 1);

    let transition = scenario
        .step(ProducerInput::CancelRequested {
            operation_id: cancelled,
        })
        .unwrap_or_else(|error| panic!("cancellation failed: {error}"));
    let replacement = execution(2);

    assert_eq!(
        transition.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert_eq!(scenario.submission_count(), 0);
    assert!(!scenario.contains_payload(PayloadId::from_raw(2)));
    assert!(scenario.contains_payload(PayloadId::from_raw(1)));
    assert!(matches!(
        scenario.terminal_result(cancelled),
        Some(ProducerCompletion::Failed(failure))
            if failure.kind() == ProducerFailureKind::Cancelled
                && failure.delivery() == DeliveryStatus::NotSent
    ));
    assert!(matches!(
        transition.effects().first(),
        Some(ProducerEffect::ReviseBatchExecution {
            previous: revoked,
            replacement: Some(next),
            removed_operation_id,
        }) if *revoked == previous && *next == replacement && *removed_operation_id == cancelled
    ));
    assert!(matches!(
        transition.effects().last(),
        Some(ProducerEffect::MaterializeBatch {
            execution: requested,
            ..
        }) if *requested == replacement
    ));

    for stale in [
        ProducerInput::BatchMaterialized {
            execution: previous,
            now: Moment::from_tick(3),
        },
        ProducerInput::BatchMaterializationFailed {
            execution: previous,
        },
        ProducerInput::DriverRejected {
            execution: previous,
        },
    ] {
        assert!(
            scenario
                .step(stale)
                .is_ok_and(|transition| transition.effects().is_empty())
        );
    }
    scenario
        .step(ProducerInput::BatchMaterialized {
            execution: replacement,
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("replacement materialization failed: {error}"));
    assert_eq!(scenario.submission_count(), 1);
    assert!(scenario.contains_payload(PayloadId::from_raw(1)));
    assert_ne!(survivor, cancelled);
}

#[test]
fn sole_member_cancellation_discards_virtual_batch_before_completion() {
    let mut scenario = ProducerScenario::new(ByteCount::new(64), 1);
    let operation_id = admit(&mut scenario, 1);
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id: BatchId::from_raw(1),
            accumulator_bytes: BYTES,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));

    scenario
        .step(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("cancellation failed: {error}"));

    assert!(!scenario.contains_batch(BatchId::from_raw(1)));
    assert!(!scenario.contains_payload(PayloadId::from_raw(1)));
    assert!(scenario.terminal_result(operation_id).is_some());
}

fn sealed_pair() -> (ProducerScenario, OperationId, OperationId, BatchExecutionId) {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("policy invalid: {error}"));
    let mut scenario = ProducerScenario::with_batch_policy(ByteCount::new(64), 2, policy);
    let first = admit(&mut scenario, 1);
    accumulate(&mut scenario, first);
    let second = admit(&mut scenario, 2);
    accumulate(&mut scenario, second);
    (scenario, first, second, execution(1))
}

fn admit(scenario: &mut ProducerScenario, payload: u64) -> OperationId {
    let payload_id = PayloadId::from_raw(payload);
    scenario
        .retain_payload(payload_id, BYTES)
        .unwrap_or_else(|error| panic!("payload retention failed: {error}"));
    let transition = scenario
        .step(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: ExplicitRecord::new(
                payload_id,
                TopicId::from_raw(7),
                PartitionIndex::from_raw(0),
                BYTES,
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    match transition.effects().first() {
        Some(ProducerEffect::AccumulateExplicit { operation_id, .. }) => *operation_id,
        effect => panic!("unexpected admission effect: {effect:?}"),
    }
}

fn accumulate(scenario: &mut ProducerScenario, operation_id: OperationId) {
    scenario
        .step(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id: BatchId::from_raw(1),
            accumulator_bytes: BYTES,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

fn execution(generation: u64) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(generation)
        .unwrap_or_else(|| panic!("execution generation must be nonzero"));
    BatchExecutionId::new(BatchId::from_raw(1), generation)
}
