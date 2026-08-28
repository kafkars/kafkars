//! Same-topic Produce settlement owns one exact post-outcome routing barrier.

use std::time::{Duration, Instant};

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, DeliveryStatus, Moment,
    ProducerAttemptFailureKind, ProducerBrokerFailureKind, ProducerInput,
};
use kafka_driver::RequestError;
use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use super::{
    super::produce_call_entries::{TrackedProduceEntries, TrackedProduceEntry},
    settlement::SettledProduceCall,
};
use crate::clock::OperationDeadline;

#[test]
fn single_transport_failure_requires_exact_broker_refresh_authority() {
    let deadline = operation_deadline(90);
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::Single(entry(1, 90, 0)),
        deadline,
        Err(RequestError::RouteUnavailable),
        Moment::from_tick(10),
        None,
    );

    assert!(matches!(
        settled.input(),
        ProducerInput::TransportFailed {
            execution: actual,
            failure: ProducerAttemptFailureKind::RouteUnavailable,
            delivery: DeliveryStatus::NotSent,
            route_refreshed: false,
            ..
        } if actual == execution(1)
    ));
    assert!(settled.route_refresh_required_for_test());
    assert_eq!(settled.operation_deadline_for_test(), deadline);
}

#[test]
fn single_broker_routing_failure_requires_exact_broker_refresh_authority() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::Single(entry(1, 90, 0)),
        operation_deadline(90),
        Ok(single_routing_failure_response()),
        Moment::from_tick(10),
        None,
    );

    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: false,
            ..
        } if actual == execution(1) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(settled.route_refresh_required_for_test());
}

#[test]
fn aggregate_with_a_later_routing_failure_requires_one_same_topic_barrier() {
    let mut settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 90, 0), entry(2, 90, 1)]),
        operation_deadline(90),
        Ok(response()),
        Moment::from_tick(10),
        None,
    );

    assert!(settled.route_refresh_required_for_test());
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerSucceeded { execution: actual, .. } if actual == execution(1)
    ));
    settled.complete_route_refresh_for_test();
    assert!(settled.advance(Moment::from_tick(12)));
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: true,
            ..
        } if actual == execution(2) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(!settled.advance(Moment::from_tick(13)));
}

#[test]
fn completed_same_topic_barrier_marks_every_later_routing_failure() {
    let mut settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 70, 0), entry(2, 70, 1), entry(3, 70, 2)]),
        operation_deadline(70),
        Ok(response_with_two_routing_failures()),
        Moment::from_tick(10),
        None,
    );

    assert!(settled.route_refresh_required_for_test());
    settled.complete_route_refresh_for_test();
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerSucceeded { execution: actual, .. } if actual == execution(1)
    ));
    assert!(settled.advance(Moment::from_tick(20)));
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: true,
            ..
        } if actual == execution(2) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(settled.advance(Moment::from_tick(41)));
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: true,
            ..
        } if actual == execution(3) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(!settled.advance(Moment::from_tick(42)));
}

#[test]
fn mixed_topic_entries_cannot_share_one_topic_barrier() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![
            entry_for("orders", 1, 70, 0),
            entry_for("payments", 2, 70, 1),
        ]),
        operation_deadline(70),
        Ok(mixed_topic_response()),
        Moment::from_tick(10),
        None,
    );

    assert!(!settled.route_refresh_required_for_test());
}

#[test]
fn aggregate_shutdown_recovery_retains_and_seals_every_execution() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 20, 0), entry(2, 20, 1), entry(3, 20, 2)]),
        operation_deadline(20),
        Ok(response_with_two_routing_failures()),
        Moment::from_tick(10),
        None,
    );

    let recovered = settled.recover_after_driver_shutdown();
    assert_eq!(
        recovered.executions().collect::<Vec<_>>(),
        vec![execution(1), execution(2), execution(3)]
    );
    recovered.seal();
}

fn operation_deadline(raw: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(raw),
        Instant::now() + Duration::from_secs(1),
    )
}

fn entry(batch: u64, deadline: u64, partition: i32) -> TrackedProduceEntry {
    entry_for("orders", batch, deadline, partition)
}

fn entry_for(topic: &str, batch: u64, deadline: u64, partition: i32) -> TrackedProduceEntry {
    TrackedProduceEntry {
        execution: execution(batch),
        deadline: Deadline::from_tick(deadline),
        topic: topic.into(),
        partition,
    }
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn response() -> ProduceResponse {
    let mut success = PartitionProduceResponse::default();
    success.index = 0;
    success.base_offset = 41;
    let mut routing_failure = PartitionProduceResponse::default();
    routing_failure.index = 1;
    routing_failure.error_code = 6;
    let mut topic = TopicProduceResponse::default();
    topic.name = "orders".into();
    topic.partition_responses = vec![success, routing_failure];
    let mut response = ProduceResponse::default();
    response.responses.push(topic);
    response
}

fn response_with_two_routing_failures() -> ProduceResponse {
    let mut response = response();
    let mut routing_failure = PartitionProduceResponse::default();
    routing_failure.index = 2;
    routing_failure.error_code = 6;
    response.responses[0]
        .partition_responses
        .push(routing_failure);
    response
}

fn mixed_topic_response() -> ProduceResponse {
    let mut orders_success = PartitionProduceResponse::default();
    orders_success.index = 0;
    orders_success.base_offset = 41;
    let mut orders = TopicProduceResponse::default();
    orders.name = "orders".into();
    orders.partition_responses.push(orders_success);

    let mut payments_failure = PartitionProduceResponse::default();
    payments_failure.index = 1;
    payments_failure.error_code = 6;
    let mut payments = TopicProduceResponse::default();
    payments.name = "payments".into();
    payments.partition_responses.push(payments_failure);

    let mut response = ProduceResponse::default();
    response.responses = vec![orders, payments];
    response
}

fn single_routing_failure_response() -> ProduceResponse {
    let mut routing_failure = PartitionProduceResponse::default();
    routing_failure.index = 0;
    routing_failure.error_code = 6;
    let mut topic = TopicProduceResponse::default();
    topic.name = "orders".into();
    topic.partition_responses.push(routing_failure);
    let mut response = ProduceResponse::default();
    response.responses.push(topic);
    response
}
