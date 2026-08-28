//! Exact-broker aggregate Produce admission and recovery evidence.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, DeliveryStatus, Moment,
    ProducerAttemptFailureKind, ProducerInput,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls},
    producer::execution::PreparedProduceSubmission,
    protocol::produce::MaterializedProduce,
};

#[test]
fn aggregate_request_accepts_every_execution_into_one_tracked_owner() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));
    let mut calls = TrackedProduceCalls::new(1);
    let deadline = operation_deadline();
    let submissions = vec![submission(1, 0, deadline), submission(2, 1, deadline)];
    let accepted = calls
        .try_reserve_for(7)
        .unwrap_or_else(|| panic!("bounded Produce slot"))
        .submit_batch(&driver, submissions, Moment::from_tick(1))
        .unwrap_or_else(|_| panic!("exact-broker aggregate admission"));

    assert_eq!(
        accepted.inputs().collect::<Vec<_>>(),
        vec![
            ProducerInput::DriverAccepted {
                execution: execution(1),
            },
            ProducerInput::DriverAccepted {
                execution: execution(2),
            },
        ]
    );
    assert_eq!(calls.in_flight_request_count(), 1);
    assert_eq!(calls.broker_in_flight_request_count(7), 1);
    accepted.confirm_receipt();

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    drop(driver);
    calls.recover_after_driver_shutdown();
    assert_eq!(
        calls.recovered()[0].executions().collect::<Vec<_>>(),
        vec![execution(1), execution(2)]
    );
    calls.seal_recovered_after_execution_unavailable();
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn invalid_exact_broker_rejects_every_execution_as_not_sent() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));
    let mut calls = TrackedProduceCalls::new(1);
    let deadline = operation_deadline();
    let submissions = vec![submission(1, 0, deadline), submission(2, 1, deadline)];
    let rejection = calls
        .try_reserve_for(-1)
        .unwrap_or_else(|| panic!("bounded Produce slot"))
        .submit_batch(&driver, submissions, Moment::from_tick(1))
        .err()
        .unwrap_or_else(|| panic!("invalid broker must reject admission"));

    assert_eq!(rejection.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        rejection.failure_kind(),
        ProducerAttemptFailureKind::Permanent
    );
    assert_eq!(
        rejection.executions().collect::<Vec<_>>(),
        vec![execution(1), execution(2)]
    );
    drop(rejection);
    assert_eq!(calls.retained_count(), 0);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn submission(
    batch: u64,
    partition: i32,
    deadline: OperationDeadline,
) -> PreparedProduceSubmission {
    PreparedProduceSubmission::from_test_parts(
        execution(batch),
        deadline,
        MaterializedProduce::from_encoded_test_parts(
            "orders",
            partition,
            Bytes::from_static(b"encoded-record-batch"),
        ),
    )
}

fn operation_deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(40),
        Instant::now() + Duration::from_secs(1),
    )
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}
