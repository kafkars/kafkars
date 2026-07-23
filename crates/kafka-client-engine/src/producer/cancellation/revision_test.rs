//! Pending, prepared, handoff-gap, and submitted cancellation scenarios.

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, ByteCount, Deadline, Moment, ProducerBatchPolicy,
    ProducerCancellationOutcome, ProducerEffect, ProducerInput,
};

use crate::producer::{
    admission_test::{admit, record},
    host_limits_test::{start, valid_limits},
};

use super::{super::ProducerHost, ProducerHostCancelError, ProducerRevisionError};

#[test]
fn awaiting_driver_revision_revokes_unarmed_and_armed_ownership() {
    for armed in [false, true] {
        let (mut host, first, second) = two_record_host();
        assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
        if armed {
            assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
            assert_eq!(host.stats().submission_deadlines, 1);
            assert!(host.pending_effects().is_empty());
        } else {
            assert!(matches!(
                host.pending_effects(),
                [ProducerEffect::SubmitProduce { .. }]
            ));
        }
        let old_bytes = host.stats().prepared_bytes;
        assert!(old_bytes > 0);

        let accepted = host
            .try_cancel_operation(first.operation_id())
            .unwrap_or_else(|error| panic!("awaiting-driver cancellation failed: {error:?}"));

        assert_eq!(
            accepted.outcome(),
            ProducerCancellationOutcome::CancelledNotSent
        );
        assert_eq!(host.stats().prepared_batches, 0);
        assert_eq!(host.stats().prepared_bytes, 0);
        assert_eq!(host.stats().submission_deadlines, 0);
        assert_eq!(host.next_deadline(), None);
        assert!(matches!(
            host.pending_effects(),
            [ProducerEffect::MaterializeBatch { execution, .. }]
                if execution.generation().get() == 2
        ));
        drop((first, second));
    }
}

#[test]
fn missing_prepared_without_pending_is_fatal_before_core_cancellation() {
    let (mut host, admitted) = driver_ready_host();
    let submission = host
        .execution
        .take_next_driver_submission()
        .unwrap_or_else(|error| panic!("handoff preflight failed: {error}"))
        .unwrap_or_else(|| panic!("one driver submission should exist"));
    let before = host.stats();

    let result = host.try_cancel_operation(admitted.operation_id());

    assert!(matches!(result, Err(ProducerHostCancelError::Invariant(_))));
    let after = host.stats();
    assert_eq!(after.core_retained_bytes, before.core_retained_bytes);
    assert_eq!(after.core_completion_slots, before.core_completion_slots);
    assert_eq!(after.store, before.store);
    assert_eq!(after.terminal_backlog, 0);
    assert!(!after.healthy);
    drop((submission, admitted));
}

#[test]
fn driver_accepted_marks_engine_submitted_before_too_late_decision() {
    let (mut host, admitted) = driver_ready_host();
    let submission = host
        .execution
        .take_next_driver_submission()
        .unwrap_or_else(|error| panic!("handoff failed: {error}"))
        .unwrap_or_else(|| panic!("one driver submission should exist"));
    let execution = submission.execution();
    host.apply_one_driver_input(
        Moment::from_tick(2),
        ProducerInput::DriverAccepted { execution },
    )
    .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));

    let accepted = host
        .try_cancel_operation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("submitted cancellation failed: {error:?}"));

    assert_eq!(accepted.outcome(), ProducerCancellationOutcome::TooLate);
    assert_eq!(host.stats().store.records, 1);
    assert_eq!(host.stats().terminal_backlog, 0);
    drop((submission, admitted));
}

#[test]
fn stale_pending_generation_fails_without_engine_or_core_mutation() {
    let (mut host, first, second) = two_record_host();
    let previous = materialize_execution(&host);
    let stale = BatchExecutionId::new(
        previous.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second generation must be valid")),
    );
    let retained_effect = ProducerEffect::MaterializeBatch {
        execution: stale,
        compression: kafka_client_core::CompressionPolicy::Uncompressed,
    };
    host.pending_effects[0] = retained_effect;
    let before = host.stats();

    let result = host.try_cancel_operation(first.operation_id());

    assert!(matches!(
        result,
        Err(ProducerHostCancelError::Invariant(
            crate::producer::ProducerHostInvariantError::Revision(
                ProducerRevisionError::StalePendingExecution {
                    expected,
                    retained,
                }
            )
        )) if expected == previous && retained == stale
    ));
    let after = host.stats();
    assert_eq!(after.store, before.store);
    assert_eq!(after.core_retained_bytes, before.core_retained_bytes);
    assert_eq!(after.core_completion_slots, before.core_completion_slots);
    assert_eq!(host.pending_effects(), &[retained_effect]);
    assert_eq!(after.terminal_backlog, 0);
    drop((first, second));
}

#[test]
fn retry_wait_preflight_requires_explicit_store_phase_with_no_prepared_owner() {
    let (mut host, admitted) = driver_ready_host();
    let submission = host
        .execution
        .take_next_driver_submission()
        .unwrap_or_else(|error| panic!("retry handoff: {error}"))
        .unwrap_or_else(|| panic!("one retry handoff"));
    let previous = submission.execution();
    let replacement = BatchExecutionId::new(
        previous.batch_id(),
        BatchExecutionGeneration::try_from_raw(2).unwrap_or_else(|| panic!("second generation")),
    );
    host.store
        .start_batch_retry(previous, replacement)
        .unwrap_or_else(|error| panic!("retry wait: {error}"));

    let plan = host
        .preflight_cancellation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("retry-wait preflight: {error:?}"));

    assert!(plan.is_some());
    assert_eq!(host.stats().prepared_batches, 0);
    assert_eq!(host.stats().submission_deadlines, 0);
    assert!(host.stats().healthy);
    drop((submission, admitted));
}

fn two_record_host() -> (
    ProducerHost,
    crate::producer::admission::AdmittedExplicit,
    crate::producer::admission::AdmittedExplicit,
) {
    let mut host = start(ready_limits(2));
    let first = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    let second = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    (host, first, second)
}

fn driver_ready_host() -> (ProducerHost, crate::producer::admission::AdmittedExplicit) {
    let mut host = start(ready_limits(1));
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    assert_eq!(host.drive_prepared(Moment::from_tick(1), 1), Ok(1));
    (host, admitted)
}

fn materialize_execution(host: &ProducerHost) -> BatchExecutionId {
    match host.pending_effects() {
        [ProducerEffect::MaterializeBatch { execution, .. }] => *execution,
        effects => panic!("expected one materialization effect, got {effects:?}"),
    }
}

fn ready_limits(max_records: usize) -> crate::producer::ProducerHostLimits {
    let policy = ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), 100)
        .unwrap_or_else(|error| panic!("ready policy invalid: {error}"));
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}
