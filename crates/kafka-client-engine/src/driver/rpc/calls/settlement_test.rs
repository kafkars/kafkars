//! Broker-aggregated Produce settlement never invents partition-refresh authority.

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

#[test]
fn single_transport_failure_retains_partition_refresh_authority() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::Single(entry(1, 90, 0)),
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
}

#[test]
fn single_broker_routing_failure_retains_partition_refresh_authority() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::Single(entry(1, 90, 0)),
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
fn aggregate_routing_failure_never_claims_partition_refresh() {
    let mut settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 90, 0), entry(2, 40, 1)]),
        Ok(response()),
        Moment::from_tick(10),
        None,
    );

    assert_eq!(settled.refresh_deadline(), None);
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerSucceeded { execution: actual, .. } if actual == execution(1)
    ));
    assert!(settled.advance(Moment::from_tick(12)));
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: false,
            ..
        } if actual == execution(2) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(!settled.advance(Moment::from_tick(13)));
}

#[test]
fn aggregate_preserves_each_routing_failure_without_shared_retry_authority() {
    let mut settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 20, 0), entry(2, 40, 1), entry(3, 70, 2)]),
        Ok(response_with_two_routing_failures()),
        Moment::from_tick(10),
        None,
    );

    assert_eq!(settled.refresh_deadline(), None);
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
            route_refreshed: false,
            ..
        } if actual == execution(2) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(settled.advance(Moment::from_tick(41)));
    assert!(matches!(
        settled.input(),
        ProducerInput::BrokerFailed {
            execution: actual,
            failure,
            route_refreshed: false,
            ..
        } if actual == execution(3) && failure.kind() == ProducerBrokerFailureKind::Routing
    ));
    assert!(!settled.advance(Moment::from_tick(42)));
}

#[test]
fn aggregate_shutdown_recovery_retains_and_seals_every_execution() {
    let settled = SettledProduceCall::from_terminal(
        TrackedProduceEntries::batch(vec![entry(1, 20, 0), entry(2, 20, 1), entry(3, 20, 2)]),
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

fn entry(batch: u64, deadline: u64, partition: i32) -> TrackedProduceEntry {
    TrackedProduceEntry {
        execution: execution(batch),
        deadline: Deadline::from_tick(deadline),
        topic: "orders".into(),
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
