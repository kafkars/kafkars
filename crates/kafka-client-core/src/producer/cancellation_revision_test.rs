//! Sealed cancellation revokes one exact execution before driver ownership.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, ByteCount, Deadline, ExplicitRecord,
    Moment, OperationId, PartitionIndex, PayloadId, ProducerAttemptFailureKind,
    ProducerBatchPolicy, ProducerCancellationOutcome, ProducerEffect, ProducerInput,
    ProducerMachine, ProducerMachineError, ProducerOperationState, TopicId,
};

const RETAINED: ByteCount = ByteCount::new(8);

#[test]
fn materializing_cancellation_revises_target_and_fences_stale_facts() {
    let (mut producer, first, cancelled, previous) = sealed_pair();
    let replacement = execution(previous.batch_id(), 2);

    let transition = cancel(&mut producer, cancelled);

    assert_eq!(
        transition.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
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
    assert!(matches!(
        producer
            .operation(first)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Materializing { .. })
    ));
    for stale in [
        ProducerInput::BatchMaterialized {
            execution: previous,
            now: Moment::from_tick(2),
        },
        ProducerInput::BatchMaterializationFailed {
            execution: previous,
        },
        ProducerInput::DriverRejected {
            execution: previous,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::Permanent,
        },
    ] {
        assert!(
            producer
                .apply(stale)
                .is_ok_and(|transition| transition.effects().is_empty())
        );
    }
    assert_eq!(
        producer.apply(ProducerInput::DriverAccepted {
            execution: previous,
        }),
        Err(ProducerMachineError::StaleDriverAcceptance {
            reported: previous,
            current: Some(replacement),
        })
    );
}

#[test]
fn awaiting_driver_cancellation_restarts_survivors_at_a_new_generation() {
    let (mut producer, first, cancelled, previous) = sealed_pair();
    producer
        .apply(ProducerInput::BatchMaterialized {
            execution: previous,
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));

    let transition = cancel(&mut producer, cancelled);
    let replacement = execution(previous.batch_id(), 2);

    assert!(matches!(
        transition.effects().first(),
        Some(ProducerEffect::ReviseBatchExecution {
            previous: revoked,
            replacement: Some(next),
            ..
        }) if *revoked == previous && *next == replacement
    ));
    assert!(matches!(
        producer
            .operation(first)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Materializing { .. })
    ));
}

#[test]
fn sole_sealed_member_revision_discards_batch_before_terminal_completion() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1);
    accumulate(&mut producer, operation_id, batch_id);
    let previous = execution(batch_id, 1);

    let transition = cancel(&mut producer, operation_id);

    assert!(matches!(
        transition.effects(),
        [
            ProducerEffect::ReviseBatchExecution {
                previous: revoked,
                replacement: None,
                removed_operation_id,
            },
            ProducerEffect::ReleasePayload { .. },
            ProducerEffect::Complete {
                operation_id: completed,
                ..
            },
        ] if *revoked == previous
            && *removed_operation_id == operation_id
            && *completed == operation_id
    ));
    assert!(!producer.batches.contains_key(&batch_id));
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
}

#[test]
fn generation_exhaustion_has_no_partial_mutation() {
    let (mut producer, first, cancelled, initial) = sealed_pair();
    let maximum = BatchExecutionGeneration::try_from_raw(u64::MAX)
        .unwrap_or_else(|| panic!("maximum generation is nonzero"));
    producer
        .batches
        .get_mut(&initial.batch_id())
        .unwrap_or_else(|| panic!("sealed batch missing"))
        .execution_generation = Some(maximum);
    let before = producer.retained_bytes();

    assert_eq!(
        producer.apply(ProducerInput::CancelRequested {
            operation_id: cancelled,
        }),
        Err(ProducerMachineError::ExecutionGenerationExhausted)
    );
    assert_eq!(producer.retained_bytes(), before);
    assert!(
        producer
            .batches
            .get(&initial.batch_id())
            .is_some_and(|batch| batch.contains(first) && batch.contains(cancelled))
    );
    assert!(matches!(
        producer
            .operation(cancelled)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Materializing { .. })
    ));
}

fn sealed_pair() -> (ProducerMachine, OperationId, OperationId, BatchExecutionId) {
    let policy = ProducerBatchPolicy::try_new(2, ByteCount::new(1_024), 20)
        .unwrap_or_else(|error| panic!("policy invalid: {error}"));
    let mut producer = ProducerMachine::with_batch_policy(ByteCount::new(64), 2, policy);
    let (first, batch_id) = admit(&mut producer, 1);
    accumulate(&mut producer, first, batch_id);
    let (second, same_batch) = admit(&mut producer, 2);
    assert_eq!(same_batch, batch_id);
    accumulate(&mut producer, second, batch_id);
    (producer, first, second, execution(batch_id, 1))
}

fn admit(producer: &mut ProducerMachine, payload: u64) -> (OperationId, BatchId) {
    let transition = producer
        .apply(ProducerInput::AdmitExplicit {
            now: Moment::from_tick(0),
            deadline: Deadline::from_tick(100),
            record: ExplicitRecord::new(
                PayloadId::from_raw(payload),
                TopicId::from_raw(7),
                PartitionIndex::from_raw(0),
                RETAINED,
            ),
        })
        .unwrap_or_else(|error| panic!("admission failed: {error}"));
    match transition.effects().first() {
        Some(ProducerEffect::AccumulateExplicit {
            operation_id,
            batch_id,
            ..
        }) => (*operation_id, *batch_id),
        effect => panic!("unexpected admission effect: {effect:?}"),
    }
}

fn accumulate(producer: &mut ProducerMachine, operation_id: OperationId, batch_id: BatchId) {
    producer
        .apply(ProducerInput::RecordAccumulated {
            operation_id,
            batch_id,
            accumulator_bytes: RETAINED,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
}

fn cancel(producer: &mut ProducerMachine, operation_id: OperationId) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("cancellation failed: {error}"))
}

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    let generation = BatchExecutionGeneration::try_from_raw(generation)
        .unwrap_or_else(|| panic!("execution generation must be nonzero"));
    BatchExecutionId::new(batch_id, generation)
}
