//! Evidence for bounded tracked Produce-call ownership.

#[cfg(test)]
mod refresh_deadline_test;

use std::{
    num::NonZeroI16,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, DeliveryStatus, Moment,
    ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerInput,
};
use kafka_driver::RequestError;
use kafka_wire::{
    ProduceResponse,
    produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse},
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, ProduceRouteRefreshPoll, TrackedProduceCalls},
    protocol::produce::MaterializedProduce,
};

#[test]
fn routing_failure_without_exact_broker_receipt_cannot_authorize_retry() {
    let mut driver = owner();
    let input = ProducerInput::BrokerFailed {
        execution: execution(7),
        now: Moment::from_tick(12),
        failure: ProducerBrokerFailure::new(
            ProducerBrokerFailureKind::Routing,
            NonZeroI16::new(6).unwrap_or_else(|| panic!("routing code is nonzero")),
        ),
        delivery: DeliveryStatus::PossiblySent,
        route_refreshed: false,
    };
    let mut calls = TrackedProduceCalls::with_missing_route_refresh_for_test(
        execution(7),
        OperationDeadline::from_core_for_test(kafka_client_core::Deadline::from_tick(50_000_000)),
        input,
    );
    let settled = calls
        .poll_next_ready(Moment::from_tick(13))
        .unwrap_or_else(|error| panic!("poll retained terminal: {error}"))
        .unwrap_or_else(|| panic!("test terminal remains retained"));

    assert_eq!(
        settled.poll_route_refresh(&driver, Moment::from_tick(13)),
        ProduceRouteRefreshPoll::Failed
    );
    assert_eq!(settled.input(), input);
    calls.discard_settled(Moment::from_tick(13));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn permits_preflight_the_exact_bounded_owner() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::new(1);
    let permit = calls
        .try_reserve_for(7)
        .unwrap_or_else(|| panic!("first bounded slot must be available"));
    submit(permit, &driver, 1);

    assert!(calls.try_reserve_for(7).is_none());

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn configured_gate_bounds_one_exact_broker() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::with_max_in_flight_requests_per_broker(3, 2);
    assert!(calls.broker_admission_available(7));

    for batch in [1, 2] {
        let permit = calls
            .try_reserve_for(7)
            .unwrap_or_else(|| panic!("bounded slot for batch {batch}"));
        submit(permit, &driver, batch);
    }

    assert!(!calls.broker_admission_available(7));
    assert!(calls.broker_admission_available(8));
    assert!(calls.try_reserve_for(7).is_none());
    assert!(calls.try_reserve_for(8).is_some());

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn exact_broker_gate_does_not_become_a_global_five_request_ceiling() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::with_max_in_flight_requests_per_broker(7, 5);
    for batch in 1..=5 {
        let permit = calls
            .try_reserve_for(7)
            .unwrap_or_else(|| panic!("bounded broker-seven slot for batch {batch}"));
        submit(permit, &driver, batch);
    }

    let permit = calls
        .try_reserve_for(8)
        .unwrap_or_else(|| panic!("another broker retains an independent slot"));
    submit(permit, &driver, 6);

    assert!(calls.try_reserve_for(7).is_none());
    assert!(calls.try_reserve_for(8).is_some());
    assert_eq!(calls.in_flight_request_count(), 6);
    assert_eq!(calls.broker_in_flight_request_count(7), 5);

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
        .try_reserve_for(7)
        .unwrap_or_else(|| panic!("bounded slot"))
        .submit(
            &driver,
            execution(1),
            deadline(),
            materialized(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"))
        .confirm_receipt();

    assert!(
        calls
            .poll_next_ready(Moment::from_tick(1))
            .unwrap_or_else(|error| panic!("poll tracked call: {error}"))
            .is_none()
    );
    assert!(calls.try_reserve_for(7).is_none());

    drop(calls);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn shutdown_recovery_retains_every_exact_accepted_execution_until_sealed() {
    let mut driver = owner();
    let mut calls = TrackedProduceCalls::new(2);
    for batch in [3, 9] {
        let permit = calls
            .try_reserve_for(7)
            .unwrap_or_else(|| panic!("bounded slot for batch {batch}"));
        submit(permit, &driver, batch);
    }
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    drop(driver);

    calls.recover_after_driver_shutdown();
    assert_eq!(
        calls
            .recovered()
            .iter()
            .map(super::calls::RecoveredProduceCallForTest::execution)
            .collect::<Vec<_>>(),
        vec![execution(3), execution(9)]
    );
    assert_eq!(calls.retained_count(), 2);

    calls.seal_recovered_after_execution_unavailable();
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn structural_response_mismatch_terminalizes_conservatively() {
    let input = normalized_terminal_input(
        execution(7),
        "orders",
        3,
        Moment::from_tick(11),
        &Ok(success_response("wrong-topic", 3)),
    );

    assert_eq!(
        input,
        ProducerInput::TransportFailed {
            execution: execution(7),
            now: Moment::from_tick(11),
            failure: kafka_client_core::ProducerAttemptFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        }
    );
}

#[test]
fn invalid_response_false_success_terminalizes_conservatively() {
    let mut response = success_response("orders", 3);
    let mut record_error = BatchIndexAndErrorMessage::default();
    record_error.batch_index = 0;
    record_error.batch_index_error_message = Some("record rejected".into());
    response.responses[0].partition_responses[0]
        .record_errors
        .push(record_error);

    let input = normalized_terminal_input(
        execution(7),
        "orders",
        3,
        Moment::from_tick(11),
        &Ok(response),
    );

    assert_eq!(
        input,
        ProducerInput::TransportFailed {
            execution: execution(7),
            now: Moment::from_tick(11),
            failure: kafka_client_core::ProducerAttemptFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            route_refreshed: false,
        }
    );
}

#[test]
fn terminal_driver_failure_preserves_execution_time_and_structure() {
    let exact = BatchExecutionId::new(
        BatchId::from_raw(8),
        BatchExecutionGeneration::try_from_raw(3).unwrap_or_else(|| panic!("third generation")),
    );
    let input = normalized_terminal_input(
        exact,
        "orders",
        3,
        Moment::from_tick(12),
        &Err(RequestError::RouteUnavailable),
    );

    assert_eq!(
        input,
        ProducerInput::TransportFailed {
            execution: exact,
            now: Moment::from_tick(12),
            failure: kafka_client_core::ProducerAttemptFailureKind::RouteUnavailable,
            delivery: DeliveryStatus::NotSent,
            route_refreshed: false,
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
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"))
        .confirm_receipt();
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn normalized_terminal_input(
    execution: BatchExecutionId,
    topic: &str,
    partition: i32,
    now: Moment,
    result: &Result<ProduceResponse, RequestError>,
) -> ProducerInput {
    match result {
        Ok(response) => crate::protocol::produce_outcome::explicit_produce_response_input(
            execution, now, topic, partition, response,
        )
        .unwrap_or_else(|failure| {
            crate::protocol::produce_outcome::produce_transport_failure_input(
                execution,
                now,
                kafka_client_core::ProducerAttemptFailureKind::InvalidResponse,
                failure.delivery(),
            )
        }),
        Err(error) => crate::protocol::produce_outcome::produce_transport_failure_input(
            execution,
            now,
            super::super::request_failure_kind(error),
            super::super::request_failure_delivery(error),
        ),
    }
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
