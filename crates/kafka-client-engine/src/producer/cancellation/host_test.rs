//! Open, sealed, terminal, and failed-tail cancellation scenarios.

use kafka_client_core::{
    ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerCancellationOutcome, ProducerEffect,
};

use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    completion::CompletionRegistryError,
    producer::{
        admission::AdmittedExplicit,
        admission_test::{admit, record},
        host_limits_test::{start, valid_limits},
    },
};

use super::{super::ProducerHost, ProducerHostCancelError};

#[test]
fn open_cancellation_releases_before_cancelled_not_sent_terminal() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );

    let accepted = host
        .try_cancel_operation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("open cancellation failed: {error:?}"));

    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_released(&host);
    let repeated = host
        .try_cancel_operation(admitted.operation_id())
        .unwrap_or_else(|error| panic!("terminal cancellation failed: {error:?}"));
    assert_eq!(
        repeated.outcome(),
        ProducerCancellationOutcome::AlreadyTerminal
    );
    assert_cancelled(admitted);
}

#[test]
fn sealed_revisions_advance_generation_then_remove_the_final_member() {
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
    let first_execution = materialize_effect(&host);

    let accepted = host
        .try_cancel_operation(first.operation_id())
        .unwrap_or_else(|error| panic!("sealed cancellation failed: {error:?}"));
    assert_eq!(
        accepted.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    let replacement = materialize_effect(&host);
    assert_eq!(replacement.batch_id(), first_execution.batch_id());
    assert_eq!(
        replacement.generation().get(),
        first_execution.generation().get() + 1
    );
    assert_eq!(host.stats().store.records, 1);
    assert_cancelled(first);

    let final_member = host
        .try_cancel_operation(second.operation_id())
        .unwrap_or_else(|error| panic!("final cancellation failed: {error:?}"));
    assert_eq!(
        final_member.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    assert_released(&host);
    assert_cancelled(second);
}

#[test]
fn terminal_interpretation_failure_withholds_the_core_outcome() {
    let mut host = start(valid_limits());
    let admitted = admit(
        &mut host,
        Moment::from_tick(0),
        Deadline::from_tick(100),
        record("orders"),
    );
    host.inject_terminal_publish_fault(CompletionRegistryError::NotifierStopped);

    let result = host.try_cancel_operation(admitted.operation_id());

    assert!(matches!(result, Err(ProducerHostCancelError::Invariant(_))));
    assert!(!host.stats().healthy);
    assert_eq!(host.stats().store.records, 0);
    assert_eq!(host.stats().store.bytes, 0);
    assert_eq!(host.stats().terminal_backlog, 1);
    drop(admitted);
}

fn materialize_effect(host: &ProducerHost) -> kafka_client_core::BatchExecutionId {
    match host.pending_effects() {
        [ProducerEffect::MaterializeBatch { execution, .. }] => *execution,
        effects => panic!("expected one materialization effect, got {effects:?}"),
    }
}

fn assert_cancelled(admitted: AdmittedExplicit) {
    let Err(ProducerDeliveryError::Failed(failure)) = admitted.into_delivery_observer().wait()
    else {
        panic!("cancelled operation should publish a failure")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::Cancelled);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
}

fn assert_released(host: &ProducerHost) {
    let stats = host.stats();
    assert_eq!(stats.store.records, 0);
    assert_eq!(stats.store.bytes, 0);
    assert_eq!(stats.store.batches, 0);
    assert_eq!(stats.prepared_batches, 0);
    assert_eq!(stats.prepared_bytes, 0);
    assert_eq!(stats.submission_deadlines, 0);
    assert_eq!(stats.pending_effects, 0);
    assert_eq!(stats.core_retained_bytes, ByteCount::new(0));
}

fn ready_limits(max_records: usize) -> crate::producer::ProducerHostLimits {
    let policy = ProducerBatchPolicy::try_new(max_records, ByteCount::new(1_024), 100)
        .unwrap_or_else(|error| panic!("ready policy invalid: {error}"));
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}
