//! Cancellation safety and stale-fact fencing during producer retry backoff.

use crate::{
    BatchTimerGeneration, Deadline, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerCancellationOutcome, ProducerCompletion, ProducerEffect, ProducerFailureKind,
    ProducerInput, ProducerOperationState,
};

use super::scenario_support::retry::{
    admit_and_accumulate, fire_retry, next, submitted, submitted_pair, transient_failure,
};

#[test]
fn sole_retry_waiting_cancellation_releases_sequence_lease_without_advancing() {
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
    producer
        .apply(ProducerInput::CompletionReclaimed { operation_id })
        .unwrap_or_else(|error| panic!("completion reclaim failed: {error}"));
    let (_, _, sealed) = admit_and_accumulate(&mut producer, 2, 5, 30);
    assert!(sealed.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { sequence, .. }
            if sequence.base_sequence() == 0 && sequence.record_count() == 1
    )));
}

#[test]
fn missing_retry_sequence_lease_rejects_cancellation_without_mutation() {
    let (mut producer, operation_id, first) = submitted(1, 2, 30);
    transient_failure(&mut producer, first, 2);
    producer
        .batches
        .get_mut(&first.batch_id())
        .unwrap_or_else(|| panic!("retry-waiting batch missing"))
        .sequence_lease = None;
    let before = format!("{producer:?}");

    assert_eq!(
        producer.apply(ProducerInput::CancelRequested { operation_id }),
        Err(crate::ProducerMachineError::ProducerIdentityFenced)
    );
    assert_eq!(format!("{producer:?}"), before);
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
            identity: crate::ProducerIdentity::try_new(7, 2)
                .unwrap_or_else(|| panic!("valid test identity")),
            sequence: crate::ProducerSequenceLease::try_new(0, 1)
                .unwrap_or_else(|| panic!("valid test sequence")),
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
