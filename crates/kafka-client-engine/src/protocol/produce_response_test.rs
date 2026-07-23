//! Evidence for strict one-partition Produce response normalization.

use kafka_client_core::{DeliveryStatus, ProducerBatchSuccess, ProducerBrokerFailureKind};
use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use super::produce_response::{
    ProduceResponseFailure, ProduceResponseProtocolFailure, normalize_explicit_produce_response,
};

const TOPIC: &str = "art-events";
const PARTITION: i32 = 7;

#[test]
fn successful_response_normalizes_acknowledgment_metadata() {
    let mut response = response();
    let partition = sole_partition_mut(&mut response);
    partition.base_offset = 42;
    partition.log_append_time_ms = 1_234;
    partition.current_leader.leader_epoch = 9;

    assert_eq!(
        normalize_explicit_produce_response(&response, TOPIC, PARTITION),
        Ok(ProducerBatchSuccess::new(42, Some(1_234), Some(9)))
    );
}

#[test]
fn every_negative_optional_metadata_sentinel_becomes_absent() {
    for (timestamp, epoch) in [(-1, -1), (i64::MIN, i32::MIN)] {
        let mut response = response();
        let partition = sole_partition_mut(&mut response);
        partition.base_offset = 8;
        partition.log_append_time_ms = timestamp;
        partition.current_leader.leader_epoch = epoch;

        assert_eq!(
            normalize_explicit_produce_response(&response, TOPIC, PARTITION),
            Ok(ProducerBatchSuccess::new(8, None, None))
        );
    }
}

#[test]
fn zero_optional_metadata_values_remain_present() {
    let mut response = response();
    let partition = sole_partition_mut(&mut response);
    partition.base_offset = 0;
    partition.log_append_time_ms = 0;
    partition.current_leader.leader_epoch = 0;

    assert_eq!(
        normalize_explicit_produce_response(&response, TOPIC, PARTITION),
        Ok(ProducerBatchSuccess::new(0, Some(0), Some(0)))
    );
}

#[test]
fn broker_failure_response_is_possibly_sent_and_preserves_normalized_code() {
    for (code, expected_kind) in [
        (6, ProducerBrokerFailureKind::Routing),
        (-123, ProducerBrokerFailureKind::Unknown),
    ] {
        let mut response = response();
        sole_partition_mut(&mut response).error_code = code;

        let Err(failure) = normalize_explicit_produce_response(&response, TOPIC, PARTITION) else {
            panic!("nonzero broker error should fail normalization");
        };
        let ProduceResponseFailure::Broker {
            failure: broker,
            delivery,
        } = failure
        else {
            panic!("broker error should remain a broker fact");
        };
        assert_eq!(delivery, DeliveryStatus::PossiblySent);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
        assert_eq!(broker.kind(), expected_kind);
        assert_eq!(broker.code(), code);
    }
}

#[test]
fn structural_response_failures_are_possibly_sent() {
    for (response, expected) in structural_failures() {
        let Err(failure) = normalize_explicit_produce_response(&response, TOPIC, PARTITION) else {
            panic!("structurally invalid response should fail normalization");
        };
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
        assert_eq!(
            failure,
            ProduceResponseFailure::Protocol {
                failure: expected,
                delivery: DeliveryStatus::PossiblySent,
            }
        );
    }
}

fn structural_failures() -> Vec<(ProduceResponse, ProduceResponseProtocolFailure)> {
    let empty_topics = ProduceResponse::default();
    let mut duplicate_topics = response();
    duplicate_topics.responses.push(topic_response());
    let mut wrong_topic = response();
    wrong_topic.responses[0].name = "other".into();
    let mut empty_partitions = response();
    empty_partitions.responses[0].partition_responses.clear();
    let mut duplicate_partitions = response();
    duplicate_partitions.responses[0]
        .partition_responses
        .push(partition_response());
    let mut wrong_partition = response();
    sole_partition_mut(&mut wrong_partition).index = 99;
    let mut negative_offset = response();
    sole_partition_mut(&mut negative_offset).base_offset = -1;

    vec![
        (
            empty_topics,
            ProduceResponseProtocolFailure::TopicCount { actual: 0 },
        ),
        (
            duplicate_topics,
            ProduceResponseProtocolFailure::TopicCount { actual: 2 },
        ),
        (
            wrong_topic,
            ProduceResponseProtocolFailure::TopicNameMismatch,
        ),
        (
            empty_partitions,
            ProduceResponseProtocolFailure::PartitionCount { actual: 0 },
        ),
        (
            duplicate_partitions,
            ProduceResponseProtocolFailure::PartitionCount { actual: 2 },
        ),
        (
            wrong_partition,
            ProduceResponseProtocolFailure::PartitionIndexMismatch { actual: 99 },
        ),
        (
            negative_offset,
            ProduceResponseProtocolFailure::NegativeBaseOffset { actual: -1 },
        ),
    ]
}

fn response() -> ProduceResponse {
    let mut response = ProduceResponse::default();
    response.responses.push(topic_response());
    response
}

fn topic_response() -> TopicProduceResponse {
    let mut topic = TopicProduceResponse::default();
    topic.name = TOPIC.into();
    topic.partition_responses.push(partition_response());
    topic
}

fn partition_response() -> PartitionProduceResponse {
    let mut partition = PartitionProduceResponse::default();
    partition.index = PARTITION;
    partition
}

fn sole_partition_mut(response: &mut ProduceResponse) -> &mut PartitionProduceResponse {
    &mut response.responses[0].partition_responses[0]
}
