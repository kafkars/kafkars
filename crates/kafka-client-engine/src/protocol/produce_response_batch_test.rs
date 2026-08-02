//! Broker-aggregated Produce response correlation and failure-isolation tests.

use kafka_wire::{
    ProduceResponse,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

use super::{
    produce_response::{ProduceResponseFailure, ProduceResponseProtocolFailure},
    produce_response_batch::{
        normalize_batched_produce_partition, validate_batched_produce_response,
    },
};

#[test]
fn batched_response_correlates_each_partition_independently() {
    let mut response = response(&[(0, 41, 0), (1, -1, 6)]);
    response.responses[0].partition_responses[1].error_code = 6;
    let index = validate_batched_produce_response(&response, 2, targets([0, 1]))
        .unwrap_or_else(|error| panic!("validate batch: {error:?}"));

    assert_eq!(
        normalize_batched_produce_partition(&response, &index, "orders", 0)
            .unwrap_or_else(|error| panic!("first partition: {error:?}"))
            .base_offset(),
        41
    );
    assert!(matches!(
        normalize_batched_produce_partition(&response, &index, "orders", 1),
        Err(ProduceResponseFailure::Broker { .. })
    ));
}

#[test]
fn batched_response_rejects_missing_or_duplicate_targets() {
    let missing = response(&[(0, 41, 0)]);
    assert_eq!(
        validate_batched_produce_response(&missing, 2, targets([0, 1])),
        Err(ProduceResponseFailure::Protocol {
            failure: ProduceResponseProtocolFailure::BatchedPartitionCount {
                expected: 2,
                actual: 1,
            },
            delivery: kafka_client_core::DeliveryStatus::PossiblySent,
        })
    );

    let duplicate = response(&[(0, 41, 0), (0, 42, 0)]);
    assert!(matches!(
        validate_batched_produce_response(&duplicate, 2, targets([0, 1])),
        Err(ProduceResponseFailure::Protocol {
            failure: ProduceResponseProtocolFailure::BatchedTargetMismatch,
            ..
        })
    ));
}

#[test]
fn batched_response_correlation_capacity_is_conservatively_possibly_sent() {
    assert_eq!(
        validate_batched_produce_response(
            &ProduceResponse::default(),
            usize::MAX,
            std::iter::empty(),
        ),
        Err(ProduceResponseFailure::Protocol {
            failure: ProduceResponseProtocolFailure::BatchedCorrelationCapacity {
                requested: usize::MAX,
            },
            delivery: kafka_client_core::DeliveryStatus::PossiblySent,
        })
    );
}

fn targets<const N: usize>(
    partitions: [i32; N],
) -> impl Iterator<Item = (std::sync::Arc<str>, i32)> {
    partitions
        .into_iter()
        .map(|partition| (std::sync::Arc::from("orders"), partition))
}

fn response(partitions: &[(i32, i64, i16)]) -> ProduceResponse {
    let mut topic = TopicProduceResponse::default();
    topic.name = "orders".into();
    topic.partition_responses = partitions
        .iter()
        .map(|(index, offset, error)| {
            let mut partition = PartitionProduceResponse::default();
            partition.index = *index;
            partition.base_offset = *offset;
            partition.error_code = *error;
            partition
        })
        .collect();
    let mut response = ProduceResponse::default();
    response.responses.push(topic);
    response
}
