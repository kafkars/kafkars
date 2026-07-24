//! Original-deadline and atomic handoff scenarios for identity acquisition.

use std::time::Instant;

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerEffect, ProducerIdentityGeneration,
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

fn ready_host() -> ProducerHost {
    let mut limits = valid_limits();
    limits.batch_policy = ProducerBatchPolicy::try_new(1, ByteCount::new(1_024), 10)
        .unwrap_or_else(|error| panic!("batch policy must be valid: {error}"));
    start(limits)
}
