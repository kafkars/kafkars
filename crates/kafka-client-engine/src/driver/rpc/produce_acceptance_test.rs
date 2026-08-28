//! Exact replacement-execution receipt coverage through real driver admission.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, Moment, ProducerInput,
};

use crate::{
    EngineConfig, clock::OperationDeadline, driver::DriverOwner,
    protocol::produce::MaterializedProduce,
};

use super::calls::TrackedProduceCalls;

#[test]
fn accepted_receipt_preserves_replacement_execution_through_real_driver_handoff() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"));
    let mut calls = TrackedProduceCalls::new(1);
    let replacement = BatchExecutionId::new(
        BatchId::from_raw(5),
        BatchExecutionGeneration::try_from_raw(3)
            .unwrap_or_else(|| panic!("third execution generation")),
    );
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(1_000_000_000),
        Instant::now() + Duration::from_secs(1),
    );
    let materialized = MaterializedProduce::from_encoded_test_parts(
        "orders",
        3,
        Bytes::from_static(b"encoded-record-batch"),
    );

    let accepted = calls
        .try_reserve_for(7)
        .unwrap_or_else(|| panic!("bounded replacement slot"))
        .submit(
            &driver,
            replacement,
            deadline,
            materialized,
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("replacement Produce admission: {error}"));

    assert_eq!(accepted.execution(), replacement);
    assert_eq!(
        accepted.driver_accepted(),
        ProducerInput::DriverAccepted {
            execution: replacement,
        }
    );
    assert_eq!(calls.retained_count(), 1);
    accepted.confirm_receipt();

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
