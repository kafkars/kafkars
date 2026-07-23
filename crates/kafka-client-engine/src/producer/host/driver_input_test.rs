//! Producer shard handoff and one-at-a-time driver outcome scenarios.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{ByteCount, Deadline, Moment, ProducerBatchPolicy, ProducerInput};

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
        ProducerInput::DriverRejected { execution },
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

fn owner() -> ProducerShardOwner {
    ProducerShardOwner::new(start(ready_limits()), Arc::new(CountingWake::default()))
}

fn ready_limits() -> crate::producer::ProducerHostLimits {
    let Ok(policy) = ProducerBatchPolicy::try_new(1, ByteCount::new(u64::MAX), 10) else {
        panic!("ready policy should be valid")
    };
    let mut limits = valid_limits();
    limits.batch_policy = policy;
    limits
}
