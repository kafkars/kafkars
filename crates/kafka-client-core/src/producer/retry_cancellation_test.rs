//! Cancellation safety and stale-fact fencing during producer retry backoff.

use crate::{
    BatchTimerGeneration, Deadline, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerCancellationOutcome, ProducerCompletion, ProducerEffect, ProducerFailureKind,
    ProducerInput, ProducerOperationState,
};

use super::retry_test_support::{fire_retry, next, submitted, submitted_pair, transient_failure};

#[test]
fn sole_retry_waiting_member_cancels_the_current_execution_and_timer() {
    let (mut producer, operation_id, first) = submitted(1, 2, 30);
    transient_failure(&mut producer, first, 2);
    let second = next(first);

    let cancelled = cancel(&mut producer, operation_id);

    assert_eq!(
        cancelled.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert!(matches!(
        cancelled.effects(),
        [
            ProducerEffect::ReviseBatchExecution {
                previous,
                replacement: None,
                removed_operation_id,
            },
            ProducerEffect::CancelBatchTimer {
                generation,
                ..
            },
            ProducerEffect::ReleasePayload { .. },
            ProducerEffect::Complete {
                completion: ProducerCompletion::Failed(failure),
                ..
            },
        ] if *previous == second
            && *removed_operation_id == operation_id
            && *generation == BatchTimerGeneration::from_raw(2)
            && failure.kind() == ProducerFailureKind::Cancelled
            && failure.delivery() == DeliveryStatus::NotSent
    ));
    for stale in [first, second] {
        assert!(stale_failure(&mut producer, stale).effects().is_empty());
    }
    for generation in [1, 2] {
        assert!(
            producer
                .apply(ProducerInput::BatchTimerFired {
                    batch_id: first.batch_id(),
                    generation: BatchTimerGeneration::from_raw(generation),
                    now: Moment::from_tick(4),
                })
                .is_ok_and(|transition| transition.effects().is_empty())
        );
    }
}

#[test]
fn shared_retry_cancellation_revises_survivors_and_replaces_timer() {
    let (mut producer, survivor, cancelled_id, first) = submitted_pair(1, 5, 30);
    let flush = producer
        .apply(ProducerInput::FlushRequested)
        .unwrap_or_else(|error| panic!("flush failed: {error}"));
    assert!(matches!(
        flush.effects(),
        [ProducerEffect::AcceptFlush { .. }]
    ));
    transient_failure(&mut producer, first, 2);
    let second = next(first);
    let third = next(second);

    let cancelled = cancel(&mut producer, cancelled_id);

    assert_eq!(
        cancelled.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert!(matches!(
        cancelled.effects(),
        [
            ProducerEffect::ReviseBatchExecution {
                previous,
                replacement: Some(replacement),
                removed_operation_id,
            },
            ProducerEffect::ArmBatchTimer {
                generation,
                deadline,
                ..
            },
            ProducerEffect::ReleasePayload { .. },
            ProducerEffect::Complete {
                completion: ProducerCompletion::Failed(failure),
                ..
            },
        ] if *previous == second
            && *replacement == third
            && *removed_operation_id == cancelled_id
            && *generation == BatchTimerGeneration::from_raw(3)
            && *deadline == Deadline::from_tick(7)
            && failure.kind() == ProducerFailureKind::Cancelled
            && failure.delivery() == DeliveryStatus::NotSent
    ));
    assert!(matches!(
        producer
            .operation(survivor)
            .map(crate::ProducerOperation::state),
        Some(ProducerOperationState::RetryWaiting { .. })
    ));
    assert!(stale_failure(&mut producer, first).effects().is_empty());
    assert!(stale_failure(&mut producer, second).effects().is_empty());
    assert!(
        producer
            .apply(ProducerInput::BatchTimerFired {
                batch_id: first.batch_id(),
                generation: BatchTimerGeneration::from_raw(2),
                now: Moment::from_tick(7),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
    assert_eq!(
        fire_retry(&mut producer, third, 3, 7).effects(),
        [ProducerEffect::MaterializeBatch {
            execution: third,
            compression: crate::CompressionPolicy::Uncompressed,
        }]
    );
    assert_eq!(producer.flush_slots(), 1);
}

fn cancel(
    producer: &mut crate::ProducerMachine,
    operation_id: crate::OperationId,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("retry cancellation failed: {error}"))
}

fn stale_failure(
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
