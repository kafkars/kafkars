//! Ambiguous-delivery fencing and post-fence retry rejection scenarios.

use crate::{
    BatchTimerGeneration, ByteCount, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerBatchSuccess, ProducerEffect, ProducerFailureKind, ProducerIdentityGeneration,
    ProducerInput, ProducerMachine, ProducerMachineError,
};

use super::scenario_support::{
    idempotence::{accumulate, admit, execution, submit},
    retry::{submitted, transient_failure},
};

#[test]
fn transient_identity_request_failure_keeps_admission_open() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 200_000_000);
    accumulate(&mut producer, operation_id, batch_id, 1);

    let retry = producer
        .apply(ProducerInput::ProducerIdentityRequestUnavailable {
            generation: ProducerIdentityGeneration::initial(),
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("transient identity failure must retry: {error}"));

    assert!(producer.admission_is_open());
    assert!(
        retry
            .effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::ArmProducerIdentityRetry { .. }))
    );
}

#[test]
fn ambiguous_batch_fences_second_submitted_batch_out_of_retry() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (first_operation, first_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, first_operation, first_batch, 1);
    submit(&mut producer, first_batch);
    let (second_operation, second_batch) = admit(&mut producer, 2, 1, 20);
    accumulate(&mut producer, second_operation, second_batch, 1);
    submit(&mut producer, second_batch);

    let invalid = producer
        .apply(ProducerInput::TransportFailed {
            execution: execution(first_batch),
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        })
        .unwrap_or_else(|error| panic!("ambiguous failure must fence: {error}"));
    assert!(!producer.admission_is_open());
    assert!(invalid.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::Complete {
            operation_id,
            completion: crate::ProducerCompletion::Failed(failure),
        } if *operation_id == first_operation
            && failure.kind() == ProducerFailureKind::InvalidResponse
            && failure.delivery() == DeliveryStatus::PossiblySent
    )));

    let second = producer
        .apply(ProducerInput::TransportFailed {
            execution: execution(second_batch),
            now: Moment::from_tick(3),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: DeliveryStatus::NotSent,
            route_refreshed: false,
        })
        .unwrap_or_else(|error| panic!("post-fence failure must settle: {error}"));
    assert!(second.effects().iter().all(|effect| !matches!(
        effect,
        ProducerEffect::RetryBatchExecution { .. }
            | ProducerEffect::ArmBatchTimer { .. }
            | ProducerEffect::MaterializeBatch { .. }
    )));
    assert_eq!(
        second
            .effects()
            .iter()
            .filter(|effect| matches!(
                effect,
                ProducerEffect::Complete {
                    operation_id,
                    completion: crate::ProducerCompletion::Failed(failure),
                } if *operation_id == second_operation
                    && failure.kind() == ProducerFailureKind::Transport
            ))
            .count(),
        1
    );
    assert!(
        producer
            .apply(ProducerInput::TransportFailed {
                execution: execution(second_batch),
                now: Moment::from_tick(4),
                failure: ProducerAttemptFailureKind::ConnectionUnavailable,
                delivery: DeliveryStatus::NotSent,
                route_refreshed: false,
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
}

#[test]
fn retry_timer_preflights_fenced_identity_before_mutating_batch_or_operations() {
    let (mut producer, _operation_id, first) = submitted(1, 3, 30);
    transient_failure(&mut producer, first, 2);
    producer.idempotence.fence();
    let before = format!("{producer:?}");

    assert_eq!(
        producer.apply(ProducerInput::BatchTimerFired {
            batch_id: first.batch_id(),
            generation: BatchTimerGeneration::from_raw(2),
            now: Moment::from_tick(5),
        }),
        Err(ProducerMachineError::ProducerIdentityFenced)
    );
    assert_eq!(format!("{producer:?}"), before);
}

#[test]
fn authoritative_success_after_another_batch_fences_still_delivers_once() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    producer.install_identity_for_test();
    let (first_operation, first_batch) = admit(&mut producer, 1, 0, 20);
    accumulate(&mut producer, first_operation, first_batch, 1);
    submit(&mut producer, first_batch);
    let (second_operation, second_batch) = admit(&mut producer, 2, 1, 20);
    accumulate(&mut producer, second_operation, second_batch, 1);
    submit(&mut producer, second_batch);

    producer
        .apply(ProducerInput::TransportFailed {
            execution: execution(first_batch),
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        })
        .unwrap_or_else(|error| panic!("ambiguous failure must fence: {error}"));
    let delivered = producer
        .apply(ProducerInput::BrokerSucceeded {
            execution: execution(second_batch),
            success: ProducerBatchSuccess::new(40, None, None),
        })
        .unwrap_or_else(|error| panic!("authoritative success must settle: {error}"));

    assert_eq!(
        delivered
            .effects()
            .iter()
            .filter(|effect| matches!(
                effect,
                ProducerEffect::Complete {
                    operation_id,
                    completion: crate::ProducerCompletion::Delivered(metadata),
                } if *operation_id == second_operation && metadata.offset() == 40
            ))
            .count(),
        1
    );
    assert!(
        producer
            .apply(ProducerInput::BrokerSucceeded {
                execution: execution(second_batch),
                success: ProducerBatchSuccess::new(40, None, None),
            })
            .is_ok_and(|transition| transition.effects().is_empty())
    );
}

#[test]
fn identity_deadline_fences_atomically_without_timing_out_later_batches() {
    assert_mixed_identity_terminal(
        |generation, now| ProducerInput::ProducerIdentityDeadlineElapsed { generation, now },
        "identity deadline",
    );
}

#[test]
fn identity_request_failure_fences_mixed_deadlines_and_ignores_stale_facts() {
    assert_mixed_identity_terminal(
        |generation, now| ProducerInput::ProducerIdentityRequestFailed { generation, now },
        "identity request failure",
    );
}

fn assert_mixed_identity_terminal(
    input: impl Fn(ProducerIdentityGeneration, Moment) -> ProducerInput,
    context: &str,
) {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (expired, expired_batch) = admit(&mut producer, 1, 0, 5);
    accumulate(&mut producer, expired, expired_batch, 1);
    let (live, live_batch) = admit(&mut producer, 2, 1, 20);
    accumulate(&mut producer, live, live_batch, 2);

    let failed = producer
        .apply(input(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(5),
        ))
        .unwrap_or_else(|error| panic!("{context} failed: {error}"));

    assert!(!producer.admission_is_open());
    assert!(producer.batches.is_empty());
    for (operation_id, expected) in [
        (expired, ProducerFailureKind::DeadlineElapsed),
        (live, ProducerFailureKind::ProducerIdentity),
    ] {
        assert!(failed.effects().iter().any(|effect| matches!(
            effect,
            ProducerEffect::Complete {
                operation_id: completed,
                completion: crate::ProducerCompletion::Failed(failure),
            } if *completed == operation_id
                && failure.kind() == expected
                && failure.delivery() == DeliveryStatus::NotSent
        )));
    }

    let state = format!("{producer:?}");
    let stale = producer
        .apply(input(
            ProducerIdentityGeneration::initial(),
            Moment::from_tick(6),
        ))
        .unwrap_or_else(|error| panic!("stale {context} failed: {error}"));
    assert!(stale.effects().is_empty());
    assert_eq!(format!("{producer:?}"), state);
}
