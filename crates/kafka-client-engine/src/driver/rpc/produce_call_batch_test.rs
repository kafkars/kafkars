//! Fail-closed evidence for unsupported broker-aggregated Produce calls.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, DeliveryStatus, Moment,
    ProducerAttemptFailureKind,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, TrackedProduceCalls},
    producer::execution::PreparedProduceSubmission,
    protocol::produce::MaterializedProduce,
};

#[test]
fn aggregate_request_is_rejected_without_exact_broker_route_authority() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));
    let mut calls = TrackedProduceCalls::new(1);
    let submissions = vec![submission(1, 0), submission(2, 1)];
    let Err(rejection) = calls
        .try_reserve()
        .unwrap_or_else(|| panic!("bounded Produce slot"))
        .submit_batch(&driver, submissions, Moment::from_tick(1))
    else {
        panic!("reviewed driver cannot admit an exact-broker aggregate")
    };

    assert_eq!(rejection.delivery(), DeliveryStatus::NotSent);
    assert_eq!(
        rejection.failure_kind(),
        ProducerAttemptFailureKind::Permanent
    );
    assert_eq!(
        rejection.executions().collect::<Vec<_>>(),
        vec![execution(1), execution(2)]
    );
    assert_eq!(calls.retained_count(), 0);

    drop(rejection);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn submission(batch: u64, partition: i32) -> PreparedProduceSubmission {
    PreparedProduceSubmission::from_test_parts(
        execution(batch),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(40),
            Instant::now() + Duration::from_secs(1),
        ),
        MaterializedProduce::from_encoded_test_parts(
            "orders",
            partition,
            Bytes::from_static(b"encoded-record-batch"),
        ),
    )
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}
