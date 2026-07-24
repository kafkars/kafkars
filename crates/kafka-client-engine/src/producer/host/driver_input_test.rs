//! Producer shard handoff and one-at-a-time driver outcome scenarios.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{
    ByteCount, Deadline, DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerBatchPolicy,
    ProducerInput, ProducerRetryPolicy,
};

use crate::producer::{
    ProducerHostInvariantError,
    admission_test::record,
    host_limits_test::{start, valid_limits},
    host_turn::ProducerTurnBudget,
    ingress::{CountingWake, ProducerShardOwner},
};
use crate::{
    ProducerDeliveryError, ProducerDeliveryFailureKind, ProducerDeliveryStatus,
    clock::OperationDeadline,
};

#[test]
fn shard_transfers_one_submission_and_interprets_one_driver_rejection() {
    let owner = owner();
    let accepted = owner
        .admission_port()
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record should be accepted: {error:?}"));
    let (admitted, operation_id, fault) = accepted.into_parts();
    assert!(operation_id.is_some());
    assert!(fault.is_ok());
    let mut data = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock the producer shard: {error:?}"));
    let budget = ProducerTurnBudget::try_new(1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("nonzero budget should be valid"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    crate::producer::test_identity::acquire_shard_if_pending(&mut data, Moment::from_tick(1));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("submission-arm turn failed: {error}"));

    let submission = data
        .take_produce_submission()
        .unwrap_or_else(|error| panic!("prepared handoff failed: {error}"))
        .unwrap_or_else(|| panic!("one prepared submission should be ready"));
    let execution = submission.execution();
    assert!(
        data.take_produce_submission()
            .unwrap_or_else(|error| panic!("empty handoff failed: {error}"))
            .is_none()
    );
    data.apply_produce_driver_input(
        Moment::from_tick(2),
        ProducerInput::DriverRejected {
            execution,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::Permanent,
        },
    )
    .unwrap_or_else(|error| panic!("driver rejection should settle: {error}"));
    let Err(ProducerDeliveryError::Failed(failure)) = admitted.wait() else {
        panic!("driver rejection should publish terminal failure");
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::DriverRejected);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    drop(submission);
}

#[test]
fn shard_rejects_non_driver_inputs_at_the_bridge() {
    let owner = owner();
    let mut data = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock the producer shard: {error:?}"));

    assert_eq!(
        data.apply_produce_driver_input(Moment::from_tick(1), ProducerInput::ExecutionUnavailable),
        Err(ProducerHostInvariantError::UnexpectedDriverInput)
    );
    assert!(!data.shard_stats().host.healthy);
}

#[test]
fn live_retry_rematerializes_fresh_execution_with_original_deadline() {
    let owner = retry_owner();
    let transport_deadline = Instant::now();
    let accepted = owner
        .admission_port()
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(30), transport_deadline),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record should be accepted: {error:?}"));
    let (admitted, _operation_id, fault) = accepted.into_parts();
    assert!(fault.is_ok());
    let mut data = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock producer shard: {error:?}"));
    let budget = ProducerTurnBudget::try_new(1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("nonzero budget should be valid"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    crate::producer::test_identity::acquire_shard_if_pending(&mut data, Moment::from_tick(1));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("submission-arm turn failed: {error}"));
    let first = data
        .take_produce_submission()
        .unwrap_or_else(|error| panic!("first handoff failed: {error}"))
        .unwrap_or_else(|| panic!("first submission missing"));
    let first_execution = first.execution();
    let original_deadline = first.deadline();
    drop(first);
    let retained = data.shard_stats().host;
    assert!(retained.store.bytes > 0);
    assert!(retained.core_retained_bytes.get() > 0);
    assert_eq!(retained.core_completion_slots, 1);
    assert_eq!(retained.completion_bindings, 1);

    data.apply_produce_driver_input(
        Moment::from_tick(2),
        ProducerInput::DriverRejected {
            execution: first_execution,
            now: Moment::from_tick(2),
            failure: ProducerAttemptFailureKind::ConnectionUnavailable,
        },
    )
    .unwrap_or_else(|error| panic!("transient rejection failed: {error}"));
    let waiting = data.shard_stats().host;
    assert_eq!(waiting.store.bytes, retained.store.bytes);
    assert_eq!(waiting.core_retained_bytes, retained.core_retained_bytes);
    assert_eq!(
        waiting.core_completion_slots,
        retained.core_completion_slots
    );
    assert_eq!(waiting.completion_bindings, retained.completion_bindings);
    assert_eq!(waiting.active_timers, 1);
    assert_eq!(waiting.pending_effects, 0);

    data.turn(Moment::from_tick(7), budget)
        .unwrap_or_else(|error| panic!("retry timer turn failed: {error}"));
    data.turn(Moment::from_tick(7), budget)
        .unwrap_or_else(|error| panic!("retry submission-arm turn failed: {error}"));
    let second = data
        .take_produce_submission()
        .unwrap_or_else(|error| panic!("replacement handoff failed: {error}"))
        .unwrap_or_else(|| panic!("replacement submission missing"));
    assert_ne!(second.execution(), first_execution);
    assert_eq!(second.execution().batch_id(), first_execution.batch_id());
    assert_eq!(second.execution().generation().get(), 2);
    assert_eq!(second.deadline(), original_deadline);
    assert_eq!(second.deadline().transport(), transport_deadline);
    let replacement = data.shard_stats().host;
    assert_eq!(replacement.store.bytes, retained.store.bytes);
    assert_eq!(
        replacement.core_retained_bytes,
        retained.core_retained_bytes
    );
    assert_eq!(
        replacement.core_completion_slots,
        retained.core_completion_slots
    );
    assert_eq!(
        replacement.completion_bindings,
        retained.completion_bindings
    );
    drop(second);
    drop(admitted);
}

#[test]
fn live_possibly_sent_failure_never_creates_replacement() {
    let owner = retry_owner();
    let accepted = owner
        .admission_port()
        .try_admit_explicit(
            Moment::from_tick(0),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(30), Instant::now()),
            record("orders"),
        )
        .unwrap_or_else(|error| panic!("record should be accepted: {error:?}"));
    let (admitted, _operation_id, fault) = accepted.into_parts();
    assert!(fault.is_ok());
    let mut data = owner
        .try_data()
        .unwrap_or_else(|error| panic!("test should lock producer shard: {error:?}"));
    let budget = ProducerTurnBudget::try_new(1, 1, 1, 1, 1)
        .unwrap_or_else(|| panic!("nonzero budget should be valid"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    crate::producer::test_identity::acquire_shard_if_pending(&mut data, Moment::from_tick(1));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("materialization turn failed: {error}"));
    data.turn(Moment::from_tick(1), budget)
        .unwrap_or_else(|error| panic!("submission-arm turn failed: {error}"));
    let submission = data
        .take_produce_submission()
        .unwrap_or_else(|error| panic!("handoff failed: {error}"))
        .unwrap_or_else(|| panic!("submission missing"));
    let execution = submission.execution();
    drop(submission);
    data.apply_produce_driver_input(
        Moment::from_tick(2),
        ProducerInput::DriverAccepted { execution },
    )
    .unwrap_or_else(|error| panic!("driver acceptance failed: {error}"));
    data.apply_produce_driver_input(
        Moment::from_tick(3),
        ProducerInput::TransportFailed {
            execution,
            now: Moment::from_tick(3),
            failure: ProducerAttemptFailureKind::LocalCapacity,
            delivery: DeliveryStatus::PossiblySent,
        },
    )
    .unwrap_or_else(|error| panic!("terminal transport fact failed: {error}"));

    assert_eq!(data.shard_stats().host.active_timers, 0);
    assert!(
        data.take_produce_submission()
            .unwrap_or_else(|error| panic!("empty handoff failed: {error}"))
            .is_none()
    );
    let Err(ProducerDeliveryError::Failed(failure)) = admitted.wait() else {
        panic!("possibly-sent transport fact should settle")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::Transport);
    assert_eq!(
        failure.delivery_status(),
        ProducerDeliveryStatus::PossiblySent
    );
}

fn owner() -> ProducerShardOwner {
    ProducerShardOwner::new(start(ready_limits()), Arc::new(CountingWake::default()))
}

fn retry_owner() -> ProducerShardOwner {
    let mut limits = ready_limits();
    limits.retry_policy = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("retry policy failed: {error}"));
    ProducerShardOwner::new(start(limits), Arc::new(CountingWake::default()))
}

fn ready_limits() -> crate::producer::ProducerHostLimits {
    let Ok(policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10) else {
        panic!("ready policy should be valid")
    };
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}
