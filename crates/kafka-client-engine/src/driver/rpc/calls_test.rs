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

use crate::{EngineConfig, clock::OperationDeadline, protocol::produce::MaterializedProduce};

use crate::driver::{DriverOwner, ProduceRouteRefreshPoll, TrackedProduceCalls};

use super::calls::{mark_route_refreshed, needs_partition_refresh, normalized_terminal_input};

#[test]
fn transient_transport_loss_uses_exact_route_refresh_when_available() {
    let execution = execution(7);
    let mut uncertain = ProducerInput::TransportFailed {
        execution,
        now: Moment::from_tick(12),
        failure: kafka_client_core::ProducerAttemptFailureKind::ConnectionUnavailable,
        delivery: DeliveryStatus::PossiblySent,
        route_refreshed: false,
    };
    assert!(needs_partition_refresh(uncertain));
    mark_route_refreshed(&mut uncertain);
    assert!(!needs_partition_refresh(uncertain));
    assert!(matches!(
        uncertain,
        ProducerInput::TransportFailed {
            route_refreshed: true,
            ..
        }
    ));

    assert!(needs_partition_refresh(ProducerInput::TransportFailed {
        execution,
        now: Moment::from_tick(12),
        failure: kafka_client_core::ProducerAttemptFailureKind::ConnectionUnavailable,
        delivery: DeliveryStatus::NotSent,
        route_refreshed: false,
    }));
    assert!(!needs_partition_refresh(ProducerInput::TransportFailed {
        execution,
        now: Moment::from_tick(12),
        failure: kafka_client_core::ProducerAttemptFailureKind::Permanent,
        delivery: DeliveryStatus::PossiblySent,
        route_refreshed: false,
    }));
}

#[test]
fn routing_failure_without_exact_partition_receipt_cannot_authorize_retry() {
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
        kafka_client_core::Deadline::from_tick(50_000_000),
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
    calls.discard_settled();
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

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
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"))
        .confirm_receipt();

    assert!(
        calls
            .poll_next_ready(Moment::from_tick(1))
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
