//! Evidence for the concrete Produce protocol-to-core join.

use core::num::NonZeroI16;

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, DeliveryStatus, Moment,
    ProducerAttemptFailureKind, ProducerBatchSuccess, ProducerBrokerFailure,
    ProducerBrokerFailureKind, ProducerInput,
};
use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use super::{
    produce_outcome::{explicit_produce_response_input, produce_transport_failure_input},
    produce_response::{ProduceResponseFailure, ProduceResponseProtocolFailure},
};

const BATCH_ID: BatchId = BatchId::from_raw(41);
const EXECUTION: BatchExecutionId =
    BatchExecutionId::new(BATCH_ID, BatchExecutionGeneration::initial());
const NOW: Moment = Moment::from_tick(73);
const TOPIC: &str = "art-events";
const PARTITION: i32 = 7;

#[test]
fn successful_response_becomes_the_correlated_core_success() {
    let mut response = response();
    let partition = sole_partition_mut(&mut response);
    partition.base_offset = 42;
    partition.log_append_time_ms = 1_234;
    partition.current_leader.leader_epoch = 9;

    assert_eq!(
        explicit_produce_response_input(EXECUTION, TOPIC, PARTITION, &response),
        Ok(ProducerInput::BrokerSucceeded {
            execution: EXECUTION,
            success: ProducerBatchSuccess::new(42, Some(1_234), Some(9)),
        })
    );
}

#[test]
fn broker_error_becomes_the_correlated_core_failure() {
    let mut response = response();
    sole_partition_mut(&mut response).error_code = 6;

    assert_eq!(
        explicit_produce_response_input(EXECUTION, TOPIC, PARTITION, &response),
        Ok(ProducerInput::BrokerFailed {
            execution: EXECUTION,
            failure: ProducerBrokerFailure::new(ProducerBrokerFailureKind::Routing, nonzero(6)),
            delivery: DeliveryStatus::PossiblySent,
        })
    );
}

#[test]
fn structural_mismatch_remains_an_exact_protocol_failure() {
    let mut response = response();
    sole_partition_mut(&mut response).index = 99;

    assert_eq!(
        explicit_produce_response_input(EXECUTION, TOPIC, PARTITION, &response),
        Err(ProduceResponseFailure::Protocol {
            failure: ProduceResponseProtocolFailure::PartitionIndexMismatch { actual: 99 },
            delivery: DeliveryStatus::PossiblySent,
        })
    );
}

#[test]
fn transport_failure_preserves_adapter_normalized_delivery_certainty() {
    for delivery in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        assert_eq!(
            produce_transport_failure_input(
                EXECUTION,
                NOW,
                ProducerAttemptFailureKind::ConnectionUnavailable,
                delivery,
            ),
            ProducerInput::TransportFailed {
                execution: EXECUTION,
                now: NOW,
                failure: ProducerAttemptFailureKind::ConnectionUnavailable,
                delivery,
            }
        );
    }
}

fn nonzero(value: i16) -> NonZeroI16 {
    match NonZeroI16::new(value) {
        Some(value) => value,
        None => panic!("test broker error must be nonzero"),
    }
}

fn response() -> ProduceResponse {
    let mut response = ProduceResponse::default();
    let mut topic = TopicProduceResponse::default();
    topic.name = TOPIC.into();
    topic.partition_responses.push(partition_response());
    response.responses.push(topic);
    response
}

fn partition_response() -> PartitionProduceResponse {
    let mut partition = PartitionProduceResponse::default();
    partition.index = PARTITION;
    partition
}

fn sole_partition_mut(response: &mut ProduceResponse) -> &mut PartitionProduceResponse {
    &mut response.responses[0].partition_responses[0]
}
