//! Original-deadline and atomic handoff scenarios for identity acquisition.

use std::time::Instant;

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerCancellationOutcome, ProducerEffect,
    ProducerIdentityGeneration, ProducerInput,
};

use crate::clock::OperationDeadline;

use super::{
    ProducerHost, ProducerIdentityHandoffError,
    admission_test::record,
    host_limits_test::{start, valid_limits},
};

#[test]
fn handoff_preserves_generation_and_original_operation_deadline() {
    let mut host = ready_host();
    let transport = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(20), transport);
    let admitted = host
        .try_admit_explicit(Moment::from_tick(0), deadline, record("orders"))
        .unwrap_or_else(|error| panic!("record admission failed: {error:?}"));

    let submission = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff failed: {error}"))
        .unwrap_or_else(|| panic!("identity acquisition must be pending"));
    let (generation, retained_deadline) = submission.into_parts();
    assert_eq!(generation, ProducerIdentityGeneration::initial());
    assert_eq!(retained_deadline.core(), Deadline::from_tick(20));
    assert_eq!(retained_deadline.transport(), transport);
    assert!(
        host.take_identity_submission()
            .unwrap_or_else(|error| panic!("empty handoff failed: {error}"))
            .is_none()
    );
    drop(admitted);
}

#[test]
fn deadline_disagreement_preserves_the_pending_effect() {
    let mut host = ready_host();
    let admitted = host
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record admission failed: {error:?}"));
    let [
        ProducerEffect::AcquireProducerIdentity {
            generation,
            deadline_operation_id,
            ..
        },
    ] = host.pending_effects()
    else {
        panic!("identity acquisition must be pending")
    };
    let changed = ProducerEffect::AcquireProducerIdentity {
        generation: *generation,
        deadline_operation_id: *deadline_operation_id,
        deadline: Deadline::from_tick(21),
    };
    host.pending_effects[0] = changed;

    assert!(matches!(
        host.take_identity_submission(),
        Err(ProducerIdentityHandoffError::DeadlineMismatch {
            effect,
            bound,
            ..
        }) if effect == Deadline::from_tick(21) && bound == Deadline::from_tick(20)
    ));
    assert_eq!(host.pending_effects(), &[changed]);
    drop(admitted);
}

#[test]
fn cancellation_before_identity_handoff_removes_the_orphaned_request() {
    let mut host = ready_host();
    let admitted = host
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record admission failed: {error:?}"));

    let cancelled = host
        .try_cancel_operation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("record cancellation failed: {error:?}"));

    assert_eq!(
        cancelled.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert!(host.pending_effects().is_empty());
    assert!(
        host.take_identity_submission()
            .unwrap_or_else(|error| panic!("cancelled handoff check failed: {error}"))
            .is_none()
    );
    let _terminal = admitted.into_delivery_observer().wait();

    let next = host
        .try_admit_explicit(
            Moment::from_tick(1),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(30), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("fresh record admission failed: {error:?}"));
    let submission = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("fresh identity handoff failed: {error}"))
        .unwrap_or_else(|| panic!("fresh identity acquisition must be pending"));
    assert_eq!(submission.into_parts().0.get(), 2);
    drop(next);
}

#[test]
fn cancellation_after_identity_handoff_fences_the_late_driver_result() {
    let mut host = ready_host();
    let admitted = host
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record admission failed: {error:?}"));
    let first = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff failed: {error}"))
        .unwrap_or_else(|| panic!("identity acquisition must be pending"));
    let first_generation = first.into_parts().0;
    host.try_cancel_operation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("record cancellation failed: {error:?}"));
    let _terminal = admitted.into_delivery_observer().wait();

    let next = host
        .try_admit_explicit(
            Moment::from_tick(1),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(30), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("fresh record admission failed: {error:?}"));
    host.apply_one_driver_input(
        Moment::from_tick(2),
        ProducerInput::ProducerIdentityAcquired {
            generation: first_generation,
            producer_id: 11,
            producer_epoch: 3,
            now: Moment::from_tick(2),
        },
    )
    .unwrap_or_else(|error| panic!("late identity result failed: {error}"));

    let fresh = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("fresh identity handoff failed: {error}"))
        .unwrap_or_else(|| panic!("fresh identity acquisition must remain pending"));
    assert_eq!(fresh.into_parts().0.get(), 2);
    drop(next);
}

#[test]
fn identity_waiter_expiry_removes_the_orphaned_request() {
    let mut host = ready_host();
    let admitted = host
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record admission failed: {error:?}"));

    assert_eq!(
        host.fire_due(Moment::from_tick(20), 1)
            .unwrap_or_else(|error| panic!("identity deadline failed: {error}")),
        1
    );
    assert!(host.pending_effects().is_empty());
    assert!(
        host.take_identity_submission()
            .unwrap_or_else(|error| panic!("expired handoff check failed: {error}"))
            .is_none()
    );
    let _terminal = admitted.into_delivery_observer().wait();
}

fn ready_host() -> ProducerHost {
    let mut limits = valid_limits();
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("batch policy must be valid: {error}"));
    start(limits)
}
