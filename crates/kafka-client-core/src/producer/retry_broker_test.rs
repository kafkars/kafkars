//! Idempotent retries for closed broker-retriable Produce responses.

use core::num::NonZeroI16;

use crate::{
    BatchTimerGeneration, Deadline, DeliveryStatus, Moment, ProducerBatchSuccess,
    ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerCancellationOutcome,
    ProducerCompletion, ProducerEffect, ProducerFailureKind, ProducerInput,
};

use super::scenario_support::retry::{
    fire_retry, has_retry, materialize_and_submit, next, submitted,
};

#[test]
fn broker_retriable_reuses_identity_sequence_and_original_deadline() {
    let (mut producer, operation_id, first) = submitted(1, 3, 30);
    let retry = broker_retry(&mut producer, first, 2);
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
                deadline: Deadline::from_tick(5),
            },
        ]
    );
    let ready = fire_retry(&mut producer, second, 2, 5);
    assert!(matches!(
        ready.effects(),
        [ProducerEffect::MaterializeBatch {
            execution,
            deadline_operation_id,
            deadline,
            identity,
            sequence,
            ..
        }] if *execution == second
            && *deadline_operation_id == operation_id
            && *deadline == Deadline::from_tick(30)
            && identity.producer_id() == 7
            && identity.producer_epoch() == 2
            && sequence.base_sequence() == 0
            && sequence.record_count() == 1
    ));

    materialize_and_submit(&mut producer, second, 5);
    let terminal = producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: second,
            success: ProducerBatchSuccess::new(91, None, Some(8)),
        })
        .unwrap_or_else(|error| panic!("replacement success failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Delivered(metadata),
        }) if *completed == operation_id && metadata.offset() == 91
    ));
}

#[test]
fn broker_retriable_exhaustion_preserves_code_and_possible_delivery() {
    let (mut producer, operation_id, first) = submitted(1, 2, 30);
    broker_retry(&mut producer, first, 2);
    let second = next(first);
    fire_retry(&mut producer, second, 2, 4);
    materialize_and_submit(&mut producer, second, 4);

    let terminal = broker_retry(&mut producer, second, 5);

    assert!(!has_retry(terminal.effects()));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Failed(failure),
        }) if *completed == operation_id
            && failure.kind() == ProducerFailureKind::BrokerRetriable
            && failure.broker_code() == Some(20)
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    assert!(!producer.admission_is_open());
}

#[test]
fn broker_retry_deadline_and_cancellation_never_regress_delivery_certainty() {
    let (mut producer, operation_id, first) = submitted(2, 5, 6);
    let retry = broker_retry(&mut producer, first, 2);
    assert!(matches!(
        retry.effects().last(),
        Some(ProducerEffect::ArmBatchTimer { deadline, .. })
            if *deadline == Deadline::from_tick(6)
    ));

    let cancellation = producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("cancellation failed: {error}"));
    assert_eq!(
        cancellation.cancellation_outcome(),
        Some(ProducerCancellationOutcome::TooLate)
    );
    assert!(cancellation.effects().is_empty());

    let terminal = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id: first.batch_id(),
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(6),
        })
        .unwrap_or_else(|error| panic!("deadline timer failed: {error}"));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            completion: ProducerCompletion::Failed(failure),
            ..
        }) if failure.kind() == ProducerFailureKind::DeadlineElapsed
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
}

#[test]
fn stalled_route_refresh_expires_with_possible_delivery_and_no_replay() {
    let (mut producer, operation_id, first) = submitted(2, 5, 6);
    let early = producer.apply(ProducerInput::RouteRefreshDeadlineElapsed {
        execution: first,
        now: Moment::from_tick(5),
        delivery: DeliveryStatus::PossiblySent,
    });
    assert!(matches!(
        early,
        Err(crate::ProducerMachineError::Transition(
            crate::TransitionError::DeadlineNotElapsed
        ))
    ));

    let terminal = producer
        .apply(ProducerInput::RouteRefreshDeadlineElapsed {
            execution: first,
            now: Moment::from_tick(6),
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("route-refresh deadline failed: {error}"));

    assert!(!has_retry(terminal.effects()));
    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            operation_id: completed,
            completion: ProducerCompletion::Failed(failure),
        }) if *completed == operation_id
            && failure.kind() == ProducerFailureKind::DeadlineElapsed
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
    assert!(!producer.admission_is_open());
}

#[test]
fn execution_loss_during_broker_retry_preserves_possible_delivery() {
    let (mut producer, _, first) = submitted(1, 2, 30);
    broker_retry(&mut producer, first, 2);

    let terminal = producer
        .apply(ProducerInput::ExecutionUnavailable)
        .unwrap_or_else(|error| panic!("execution loss failed: {error}"));

    assert!(matches!(
        terminal.effects().last(),
        Some(ProducerEffect::Complete {
            completion: ProducerCompletion::Failed(failure),
            ..
        }) if failure.kind() == ProducerFailureKind::ExecutionUnavailable
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
}

fn broker_retry(
    producer: &mut crate::ProducerMachine,
    execution: crate::BatchExecutionId,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::BrokerFailed {
            execution,
            now: Moment::from_tick(now),
            failure: retriable_failure(),
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        })
        .unwrap_or_else(|error| panic!("broker retriable failure failed: {error}"))
}

fn retriable_failure() -> ProducerBrokerFailure {
    let code = NonZeroI16::new(20).unwrap_or_else(|| panic!("test code must be nonzero"));
    ProducerBrokerFailure::new(ProducerBrokerFailureKind::Retriable, code)
}
