//! Definitely-unsent retry, generation fencing, deadline, and barrier scenarios.

use crate::{
    BatchExecutionGeneration, BatchExecutionId, BatchTimerGeneration, ByteCount, Deadline,
    DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerCompletion, ProducerEffect,
    ProducerFailureKind, ProducerInput, ProducerMachineError, ProducerOperationState,
    TransitionError,
};

use super::scenario_support::retry::{
    RETAINED, fire_retry, materialize_and_submit, next, submitted, transient_failure,
};

#[test]
fn definitely_unsent_transient_failure_retries_with_a_fresh_execution() {
    let (mut producer, _operation_id, first) = submitted(2, 5, 30);
    let retry = transient_failure(&mut producer, first, 2);
    let second = next(first);
    assert_eq!(
        retry.effects(),
        [
            ProducerEffect::RetryBatchExecution {
                previous: first,
                replacement: second,
            },
            ProducerEffect::ArmBatchTimer {
                batch_id: first.batch_id(),
                generation: BatchTimerGeneration::from_raw(2),
                deadline: Deadline::from_tick(7),
            },
        ]
    );
    assert_eq!(producer.retained_bytes(), RETAINED);
    assert_eq!(producer.completion_slots(), 1);
    assert!(stale_terminal(&mut producer, first).effects().is_empty());
    assert!(
        producer
            .apply(ProducerInput::BatchTimerFired {
                batch_id: first.batch_id(),
                generation: BatchTimerGeneration::from_raw(1),
                now: Moment::from_tick(7),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
    assert_eq!(
        producer.apply(ProducerInput::BatchTimerFired {
            batch_id: first.batch_id(),
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(6),
        }),
        Err(crate::ProducerMachineError::Transition(
            TransitionError::DeadlineNotElapsed
        ))
    );
    assert_eq!(
        producer
            .apply(ProducerInput::BatchTimerFired {
                batch_id: first.batch_id(),
                generation: BatchTimerGeneration::from_raw(2),
                now: Moment::from_tick(7),
            })
            .unwrap_or_else(|error| panic!("retry timer failed: {error}"))
            .effects(),
        [ProducerEffect::MaterializeBatch {
            execution: second,
            compression: crate::CompressionPolicy::Uncompressed,
            identity: crate::ProducerIdentity::try_new(7, 2)
                .unwrap_or_else(|| panic!("valid test identity")),
            sequence: crate::ProducerSequenceLease::try_new(0, 1)
                .unwrap_or_else(|| panic!("valid test sequence")),
        }]
    );
}

#[test]
fn retry_bound_exhaustion_settles_without_reusing_an_execution() {
    let (mut producer, operation_id, first) = submitted(1, 3, 30);
    let second = next(first);
    transient_failure(&mut producer, first, 2);
    let ready = fire_retry(&mut producer, second, 2, 5);
    assert!(matches!(
        ready.effects(),
        [ProducerEffect::MaterializeBatch {
            execution,
            ..
        }] if *execution == second
    ));
    materialize_and_submit(&mut producer, second, 5);
    let terminal = transient_failure(&mut producer, second, 6);
    assert!(matches!(
        terminal.effects(),
        [
            ProducerEffect::ReleaseBatch { .. },
            ProducerEffect::ReleasePayload { .. },
            ProducerEffect::Complete {
                operation_id: completed,
                completion: ProducerCompletion::Failed(failure),
            },
        ] if *completed == operation_id
            && failure.kind() == ProducerFailureKind::Transport
            && failure.delivery() == DeliveryStatus::NotSent
    ));
    assert_eq!(producer.retained_bytes(), ByteCount::new(0));
    assert_eq!(producer.completion_slots(), 1);
}

#[test]
fn retry_identity_exhaustion_has_no_partial_mutation() {
    let (mut producer, operation_id, first) = submitted(1, 3, 30);
    let maximum_generation = BatchExecutionGeneration::try_from_raw(u64::MAX)
        .unwrap_or_else(|| panic!("maximum execution generation is nonzero"));
    let maximum = BatchExecutionId::new(first.batch_id(), maximum_generation);
    producer
        .batches
        .get_mut(&first.batch_id())
        .unwrap_or_else(|| panic!("submitted batch missing"))
        .execution_generation = Some(maximum_generation);

    assert_eq!(
        producer.apply(ProducerInput::TransportFailed {
            execution: maximum,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: DeliveryStatus::NotSent,
        }),
        Err(ProducerMachineError::ExecutionGenerationExhausted)
    );
    assert_eq!(producer.retained_bytes(), RETAINED);
    assert!(matches!(
        producer
            .operation(operation_id)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Submitted { .. })
    ));
}

#[test]
fn retry_timer_identity_exhaustion_has_no_partial_mutation() {
    let (mut producer, operation_id, first) = submitted(1, 3, 30);
    producer
        .batches
        .get_mut(&first.batch_id())
        .unwrap_or_else(|| panic!("submitted batch missing"))
        .timer_generation = BatchTimerGeneration::from_raw(u64::MAX);

    assert_eq!(
        producer.apply(ProducerInput::TransportFailed {
            execution: first,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: DeliveryStatus::NotSent,
        }),
        Err(ProducerMachineError::TimerGenerationExhausted)
    );
    assert_eq!(producer.retained_bytes(), RETAINED);
    assert!(matches!(
        producer
            .operation(operation_id)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::Submitted { .. })
    ));
}

fn stale_terminal(
    producer: &mut crate::ProducerMachine,
    execution: crate::BatchExecutionId,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::TransportFailed {
            execution,
            now: Moment::from_tick(3),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("stale terminal failed: {error}"))
}
