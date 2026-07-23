//! Evidence for bounded tracked Produce-call ownership.

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, DeliveryStatus, Moment, ProducerInput,
};
use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use crate::{EngineConfig, clock::OperationDeadline, protocol::produce::MaterializedProduce};

use crate::driver::DriverOwner;

use super::{TrackedProduceCalls, calls::normalized_terminal_input};

#[test]
fn permits_preflight_the_exact_bounded_owner() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::new(1);
    let permit = calls
        .try_reserve()
        .unwrap_or_else(|| panic!("first bounded slot must be available"));
    submit(permit, &driver, 1);

    assert!(calls.try_reserve().is_none());

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn pending_call_remains_owned_when_a_poll_has_no_result() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("bounded slot"))
        .submit(
            &driver,
            execution(1),
            deadline(),
            materialized(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));

    assert!(
        calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("poll tracked call: {error}"))
            .is_none()
    );
    assert!(calls.try_reserve().is_none());

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn structural_response_mismatch_terminalizes_conservatively() {
    let input = normalized_terminal_input(
        execution(7),
        "orders",
        3,
        &Ok(success_response("wrong-topic", 3)),
    );

    assert_eq!(
        input,
        ProducerInput::TransportFailed {
            batch_id: BatchId::from_raw(7),
            delivery: DeliveryStatus::PossiblySent,
        }
    );
}

fn submit(permit: super::calls::ProduceCallPermit<'_>, driver: &DriverOwner, batch: u64) {
    permit
        .submit(
            driver,
            execution(batch),
            deadline(),
            materialized(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(1_000_000_000),
        Instant::now() + Duration::from_secs(1),
    )
}

fn materialized() -> crate::protocol::produce::MaterializedProduce {
    MaterializedProduce::from_encoded_test_parts(
        "orders",
        3,
        Bytes::from_static(b"encoded-record-batch"),
    )
}

fn success_response(topic: &str, partition: i32) -> ProduceResponse {
    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = partition;
    partition_response.base_offset = 42;
    let mut topic_response = TopicProduceResponse::default();
    topic_response.name = topic.into();
    topic_response.partition_responses.push(partition_response);
    let mut response = ProduceResponse::default();
    response.responses.push(topic_response);
    response
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
