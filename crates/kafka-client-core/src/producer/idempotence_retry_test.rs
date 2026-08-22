//! Producer-identity coordinator-load retry, fencing, and deadline scenarios.

use core::num::NonZeroI16;

use crate::{
    ByteCount, Deadline, Moment, ProducerCancellationOutcome, ProducerEffect, ProducerFailureKind,
    ProducerIdentityGeneration, ProducerInput, ProducerMachine, ProducerMachineError,
};

use super::scenario_support::idempotence::{accumulate, admit};

const BACKOFF: u64 = 100_000_000;

#[test]
fn coordinator_load_retry_advances_generation_and_preserves_live_deadline() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, BACKOFF + 50);
    accumulate(&mut producer, operation_id, batch_id, 1);

    let scheduled = coordinator_load(&mut producer, ProducerIdentityGeneration::initial(), 10);
    let Some(ProducerEffect::ArmProducerIdentityRetry { schedule }) =
        scheduled.effects().first().copied()
    else {
        panic!("coordinator load must arm one identity retry")
    };
    assert_eq!(schedule.failed_generation().get(), 1);
    assert_eq!(schedule.retry_generation().get(), 2);
    assert_eq!(schedule.not_before(), Deadline::from_tick(BACKOFF + 10));

    let state = format!("{producer:?}");
    let stale = producer
        .apply(ProducerInput::ProducerIdentityAcquired {
            generation: ProducerIdentityGeneration::initial(),
            producer_id: 9,
            producer_epoch: 1,
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("stale acquisition must be ignored: {error}"));
    assert!(stale.effects().is_empty());
    assert_eq!(format!("{producer:?}"), state);

    assert_eq!(
        producer.apply(ProducerInput::ProducerIdentityRetryDue {
            schedule,
            now: Moment::from_tick(BACKOFF + 9),
        }),
        Err(ProducerMachineError::ProducerIdentityRetryNotDue)
    );
    let due = producer
        .apply(ProducerInput::ProducerIdentityRetryDue {
            schedule,
            now: Moment::from_tick(BACKOFF + 10),
        })
        .unwrap_or_else(|error| panic!("identity retry must become due: {error}"));
    assert!(matches!(
        due.effects(),
        [ProducerEffect::AcquireProducerIdentity {
            generation,
            deadline_operation_id,
            deadline,
        }] if *generation == schedule.retry_generation()
            && *deadline_operation_id == operation_id
            && *deadline == Deadline::from_tick(BACKOFF + 50)
    ));
    let state = format!("{producer:?}");
    assert_eq!(
        producer.apply(ProducerInput::ProducerIdentityRetryDue {
            schedule,
            now: Moment::from_tick(BACKOFF + 10),
        }),
        Err(ProducerMachineError::ProducerIdentityRetryScheduleMismatch)
    );
    assert_eq!(format!("{producer:?}"), state);

    let acquired = producer
        .apply(ProducerInput::ProducerIdentityAcquired {
            generation: schedule.retry_generation(),
            producer_id: 9,
            producer_epoch: 1,
            now: Moment::from_tick(BACKOFF + 11),
        })
        .unwrap_or_else(|error| panic!("retried identity must settle: {error}"));
    assert!(acquired.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::MaterializeBatch { execution, .. }
            if execution.batch_id() == batch_id
    )));
}

#[test]
fn repeated_coordinator_load_reserves_a_fresh_generation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, BACKOFF * 3);
    accumulate(&mut producer, operation_id, batch_id, 1);
    let first = schedule(coordinator_load(
        &mut producer,
        ProducerIdentityGeneration::initial(),
        0,
    ));
    producer
        .apply(ProducerInput::ProducerIdentityRetryDue {
            schedule: first,
            now: Moment::from_tick(BACKOFF),
        })
        .unwrap_or_else(|error| panic!("first retry must become due: {error}"));

    let second = schedule(coordinator_load(
        &mut producer,
        first.retry_generation(),
        BACKOFF + 1,
    ));
    assert_eq!(second.failed_generation(), first.retry_generation());
    assert_eq!(second.retry_generation().get(), 3);
}

#[test]
fn cancellation_of_every_waiter_returns_identity_to_uninitialized() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, BACKOFF + 50);
    accumulate(&mut producer, operation_id, batch_id, 1);
    let retry = schedule(coordinator_load(
        &mut producer,
        ProducerIdentityGeneration::initial(),
        0,
    ));

    let cancelled = producer
        .apply(ProducerInput::CancelRequested { operation_id })
        .unwrap_or_else(|error| panic!("identity waiter cancellation failed: {error}"));
    assert_eq!(
        cancelled.cancellation_outcome(),
        Some(ProducerCancellationOutcome::CancelledNotSent)
    );
    assert!(!producer.batches.contains_key(&batch_id));
    let due = producer
        .apply(ProducerInput::ProducerIdentityRetryDue {
            schedule: retry,
            now: Moment::from_tick(BACKOFF),
        })
        .unwrap_or_else(|error| panic!("empty identity retry must settle: {error}"));
    assert!(due.effects().is_empty());
    assert!(producer.idempotence.is_uninitialized());
    assert!(producer.admission_is_open());
}

#[test]
fn retry_due_selects_the_surviving_deadline_owner_after_cancellation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (cancelled, cancelled_batch) = admit(&mut producer, 1, 0, BACKOFF + 20);
    accumulate(&mut producer, cancelled, cancelled_batch, 1);
    let (survivor, survivor_batch) = admit(&mut producer, 2, 1, BACKOFF + 50);
    accumulate(&mut producer, survivor, survivor_batch, 2);
    let retry = schedule(coordinator_load(
        &mut producer,
        ProducerIdentityGeneration::initial(),
        0,
    ));
    producer
        .apply(ProducerInput::CancelRequested {
            operation_id: cancelled,
        })
        .unwrap_or_else(|error| panic!("former deadline owner cancellation failed: {error}"));

    let due = producer
        .apply(ProducerInput::ProducerIdentityRetryDue {
            schedule: retry,
            now: Moment::from_tick(BACKOFF),
        })
        .unwrap_or_else(|error| panic!("surviving identity retry failed: {error}"));
    assert!(matches!(
        due.effects(),
        [ProducerEffect::AcquireProducerIdentity {
            deadline_operation_id,
            deadline,
            ..
        }] if *deadline_operation_id == survivor
            && *deadline == Deadline::from_tick(BACKOFF + 50)
    ));
}

#[test]
fn identity_retry_deadline_preserves_existing_atomic_fencing() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 2);
    let (expired, expired_batch) = admit(&mut producer, 1, 0, 50);
    accumulate(&mut producer, expired, expired_batch, 1);
    let (later, later_batch) = admit(&mut producer, 2, 1, 200);
    accumulate(&mut producer, later, later_batch, 2);
    let retry = schedule(coordinator_load(
        &mut producer,
        ProducerIdentityGeneration::initial(),
        0,
    ));
    assert_eq!(retry.not_before(), Deadline::from_tick(50));

    let terminal = producer
        .apply(ProducerInput::ProducerIdentityRetryDue {
            schedule: retry,
            now: Moment::from_tick(50),
        })
        .unwrap_or_else(|error| panic!("deadline-owned retry must settle: {error}"));
    assert!(!producer.admission_is_open());
    assert!(producer.batches.is_empty());
    for (operation_id, kind) in [
        (expired, ProducerFailureKind::DeadlineElapsed),
        (later, ProducerFailureKind::ProducerIdentity),
    ] {
        assert!(terminal.effects().iter().any(|effect| matches!(
            effect,
            ProducerEffect::Complete {
                operation_id: completed,
                completion: crate::ProducerCompletion::Failed(failure),
            } if *completed == operation_id && failure.kind() == kind
        )));
    }
}

#[test]
fn coordinator_load_observed_at_deadline_never_arms_zero_backoff() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 5);
    accumulate(&mut producer, operation_id, batch_id, 1);

    let terminal = coordinator_load(&mut producer, ProducerIdentityGeneration::initial(), 5);
    assert!(!producer.admission_is_open());
    assert!(
        !terminal
            .effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::ArmProducerIdentityRetry { .. }))
    );
    assert!(terminal.effects().iter().any(|effect| matches!(
        effect,
        ProducerEffect::Complete {
            operation_id: completed,
            completion: crate::ProducerCompletion::Failed(failure),
        } if *completed == operation_id
            && failure.kind() == ProducerFailureKind::DeadlineElapsed
    )));
}

#[test]
fn identity_retry_deadline_preflight_has_no_partial_state_mutation() {
    let mut producer = ProducerMachine::new(ByteCount::new(64), 1);
    let (operation_id, batch_id) = admit(&mut producer, 1, 0, 50);
    accumulate(&mut producer, operation_id, batch_id, 1);
    let retry = schedule(coordinator_load(
        &mut producer,
        ProducerIdentityGeneration::initial(),
        0,
    ));
    let removed = producer.records.remove(&operation_id);
    assert!(removed.is_some());
    let state = format!("{producer:?}");

    assert_eq!(
        producer.apply(ProducerInput::ProducerIdentityRetryDue {
            schedule: retry,
            now: Moment::from_tick(50),
        }),
        Err(ProducerMachineError::UnknownOperation)
    );
    assert_eq!(format!("{producer:?}"), state);
    assert_eq!(producer.idempotence.retry_schedule(), Some(retry));
}

fn coordinator_load(
    producer: &mut ProducerMachine,
    generation: ProducerIdentityGeneration,
    now: u64,
) -> crate::ProducerTransition {
    producer
        .apply(ProducerInput::ProducerIdentityFailed {
            generation,
            broker_code: NonZeroI16::new(14),
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("coordinator load transition failed: {error}"))
}

fn schedule(transition: crate::ProducerTransition) -> crate::ProducerIdentityRetrySchedule {
    let Some(ProducerEffect::ArmProducerIdentityRetry { schedule }) =
        transition.into_effects().into_iter().next()
    else {
        panic!("identity retry schedule missing")
    };
    schedule
}
