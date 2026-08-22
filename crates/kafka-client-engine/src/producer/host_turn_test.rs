//! Fair host-turn scheduling, identity backoff, expiry, and reclamation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerEffect, ProducerInput,
};

use crate::{ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus};

use super::{
    ProducerHostInvariantError,
    admission_test::{admit, admit_without_identity, record},
    host_limits_test::{start, valid_limits},
    host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
};

#[test]
fn every_stage_requires_an_explicit_nonzero_budget() {
    assert!(ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 1).is_some());
    assert!(ProducerTurnBudget::try_new(0, 1, 1, 1, 1, 1).is_none());
    assert!(ProducerTurnBudget::try_new(1, 0, 1, 1, 1, 1).is_none());
    assert!(ProducerTurnBudget::try_new(1, 1, 0, 1, 1, 1).is_none());
    assert!(ProducerTurnBudget::try_new(1, 1, 1, 0, 1, 1).is_none());
    assert!(ProducerTurnBudget::try_new(1, 1, 1, 1, 0, 1).is_none());
    assert!(ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 0).is_none());
}

#[test]
fn busy_timer_stage_cannot_starve_prepared_work() {
    let mut host = start(valid_limits());
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(200),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(200),
        record("payments"),
    );
    assert_eq!(host.fire_due(Moment::from_tick(100), 1), Ok(1));
    super::test_identity::acquire_host_if_pending(&mut host, Moment::from_tick(100));

    let outcome = host
        .turn(Moment::from_tick(100), one_each())
        .unwrap_or_else(|error| panic!("bounded turn should run: {error}"));

    assert_eq!(outcome.batch_timers, 1);
    assert_eq!(outcome.prepared_effects, 1);
    assert_eq!(outcome.submission_expiries, 0);
    assert_eq!(outcome.completion_retries, 0);
    assert_eq!(outcome.reclaim_attempts, 0);
    assert_eq!(outcome.next_deadline, Some(Deadline::from_tick(200)));
    assert!(outcome.runnable_work);
    assert!(!outcome.blocked_work);
    assert_eq!(host.stats().active_timers, 0);
    assert_eq!(host.stats().prepared_batches, 1);
    assert!(
        host.pending_effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::SubmitProduce { .. }))
    );
    assert!(
        host.pending_effects()
            .iter()
            .any(|effect| matches!(effect, ProducerEffect::MaterializeBatch { .. }))
    );
    drop(first);
    drop(second);
}

#[test]
fn prepared_submission_expires_and_reclaims_across_bounded_turns() {
    let mut host = start(immediate_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(5),
        record("orders"),
    );
    let operation_id = admitted.operation_id();

    let materialized: ProducerTurnOutcome = host
        .turn(Moment::from_tick(1), one_each())
        .unwrap_or_else(|error| panic!("materialization turn should run: {error}"));
    assert_eq!(materialized.prepared_effects, 1);
    assert_eq!(materialized.submission_expiries, 0);
    assert!(
        materialized.runnable_work,
        "a newly generated SubmitProduce effect requires an immediate follow-up turn"
    );
    assert!(!materialized.blocked_work);
    assert_eq!(materialized.next_deadline, Some(Deadline::from_tick(5)));
    assert!(matches!(
        host.pending_effects(),
        [ProducerEffect::SubmitProduce { .. }]
    ));

    let expired = host
        .turn(Moment::from_tick(5), one_each())
        .unwrap_or_else(|error| panic!("submission expiry turn should run: {error}"));
    assert_eq!(expired.prepared_effects, 1);
    assert_eq!(expired.submission_expiries, 1);
    assert_eq!(expired.next_deadline, None);
    assert_eq!(host.stats().store.bytes, 0);
    assert_eq!(host.stats().prepared_bytes, 0);
    let observer = admitted.into_delivery_observer();
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("pre-driver expiry should fail delivery")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);

    let reclaimed = host
        .turn(Moment::from_tick(5), one_each())
        .unwrap_or_else(|error| panic!("reclaim turn should run: {error}"));
    assert_eq!(reclaimed.reclaim_attempts, 1);
    assert_eq!(host.stats().core_completion_slots, 0);
    assert_eq!(host.bindings.completion(operation_id), None);

    let idle = host
        .turn(Moment::from_tick(5), one_each())
        .unwrap_or_else(|error| panic!("idle turn should run: {error}"));
    assert!(!idle.runnable_work);
    assert!(!idle.blocked_work);
}

#[test]
fn identity_retry_waits_for_backoff_then_hands_off_one_fresh_generation() {
    const BACKOFF: u64 = 100_000_000;
    let mut host = start(immediate_limits());
    let admitted = admit_without_identity(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(BACKOFF + 50),
        record("orders"),
    );
    let submission = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff should agree: {error}"))
        .unwrap_or_else(|| panic!("initial identity request must be pending"));
    let (generation, _deadline) = submission.into_parts();
    host.apply_one_driver_input(
        Moment::from_tick(10),
        ProducerInput::ProducerIdentityFailed {
            generation,
            broker_code: NonZeroI16::new(14),
            now: Moment::from_tick(10),
        },
    )
    .unwrap_or_else(|error| panic!("coordinator load must schedule retry: {error}"));

    let waiting = host
        .turn(Moment::from_tick(20), one_each())
        .unwrap_or_else(|error| panic!("future retry turn failed: {error}"));
    assert_eq!(waiting.prepared_effects, 0);
    assert_eq!(
        waiting.next_deadline,
        Some(Deadline::from_tick(BACKOFF + 10))
    );
    assert!(!waiting.runnable_work);

    let due = host
        .turn(Moment::from_tick(BACKOFF + 10), one_each())
        .unwrap_or_else(|error| panic!("due retry turn failed: {error}"));
    assert_eq!(due.prepared_effects, 1);
    assert!(due.runnable_work);
    let retry = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("retry handoff should agree: {error}"))
        .unwrap_or_else(|| panic!("due retry must request one identity"));
    let (retry_generation, retry_deadline) = retry.into_parts();
    assert_eq!(retry_generation.get(), generation.get() + 1);
    assert_eq!(retry_deadline.core(), Deadline::from_tick(BACKOFF + 50));
    drop(admitted);
}

#[test]
fn identity_retry_wins_equal_deadline_and_fences_every_waiter_atomically() {
    let mut host = start(immediate_limits());
    let expired = admit_without_identity(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(50),
        record("orders"),
    );
    let later = admit_without_identity(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(200),
        record("payments"),
    );
    let submission = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff should agree: {error}"))
        .unwrap_or_else(|| panic!("initial identity request must be pending"));
    let (generation, _deadline) = submission.into_parts();
    host.apply_one_driver_input(
        Moment::from_tick(0),
        ProducerInput::ProducerIdentityFailed {
            generation,
            broker_code: NonZeroI16::new(14),
            now: Moment::from_tick(0),
        },
    )
    .unwrap_or_else(|error| panic!("coordinator load must schedule retry: {error}"));

    let due = host
        .turn(Moment::from_tick(50), one_each())
        .unwrap_or_else(|error| panic!("equal deadline turn failed: {error}"));
    assert_eq!(due.prepared_effects, 1);
    assert_eq!(due.batch_timers, 0);
    assert!(
        host.take_identity_submission()
            .unwrap_or_else(|error| panic!("identity handoff should agree: {error}"))
            .is_none()
    );

    let Err(ProducerDeliveryError::Failed(expired_failure)) =
        expired.into_delivery_observer().wait()
    else {
        panic!("expired identity waiter must fail")
    };
    assert_eq!(
        expired_failure.kind(),
        ProducerDeliveryFailureKind::DeadlineElapsed
    );
    let Err(ProducerDeliveryError::Failed(later_failure)) = later.into_delivery_observer().wait()
    else {
        panic!("later identity waiter must be fenced atomically")
    };
    assert_eq!(
        later_failure.kind(),
        ProducerDeliveryFailureKind::ProducerIdentity
    );
    assert_eq!(
        later_failure.delivery_status(),
        ProducerDeliveryStatus::NotSent
    );
}

#[test]
fn poisoned_host_returns_the_same_invariant_without_running_stages() {
    let mut host = start(valid_limits());
    let invariant = ProducerHostInvariantError::PendingEffectCapacity;
    assert_eq!(host.poison(invariant), invariant);

    assert_eq!(host.turn(Moment::from_tick(10), one_each()), Err(invariant));
    assert_eq!(host.stats().active_timers, 0);
    assert_eq!(host.stats().pending_effects, 0);
    assert!(!host.stats().healthy);
}

fn one_each() -> ProducerTurnBudget {
    ProducerTurnBudget::try_new(1, 1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("one is a valid stage budget"))
}

fn immediate_limits() -> super::ProducerHostLimits {
    let Ok(policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 100) else {
        panic!("immediate policy should be valid")
    };
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}
