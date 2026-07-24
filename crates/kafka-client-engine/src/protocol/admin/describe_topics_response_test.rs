//! Generated Metadata response normalization scenarios for `DescribeTopics`.

use core::num::NonZeroI16;

use kafka_client_core::{DescribeTopicResult, DescribeTopicsInput, DescribeTopicsPlan};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::Uuid;

use super::describe_topics_response::{
    DescribeTopicsProtocolFailure, normalize_describe_topics_response,
};

#[test]
fn results_return_in_request_order_with_sorted_partitions_and_exact_codes() {
    let plan = plan();
    let mut orders = described_topic("orders", [partition(2, -811), partition(0, 0)]);
    orders.topic_id = Uuid::from_bytes([9; 16]);
    let mut response = MetadataResponse::default();
    response.topics = vec![failed_topic("audit", -731), orders];

    let input = normalize_describe_topics_response(&plan, &response, 128 * 1024)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let DescribeTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("expected per-topic results");
    };
    assert_eq!(outcomes[0].topic(), "orders");
    let (_, DescribeTopicResult::Described(description)) = outcomes[0].clone().into_parts() else {
        panic!("orders should be described");
    };
    assert_eq!(description.topic_id(), Some([9; 16]));
    assert_eq!(description.partitions()[0].partition_index(), 0);
    assert_eq!(description.partitions()[1].partition_index(), 2);
    assert_eq!(description.partitions()[1].error_code(), Some(-811));
    let (_, DescribeTopicResult::Failed(error)) = outcomes[1].clone().into_parts() else {
        panic!("audit should retain its broker failure");
    };
    assert_eq!(error.code(), -731);
}

#[test]
fn zero_topic_id_sentinel_becomes_none_without_leaking_wire_uuid() {
    let mut response = MetadataResponse::default();
    response.topics = vec![
        described_topic("orders", [partition(0, 0)]),
        failed_topic("audit", 3),
    ];
    let input = normalize_describe_topics_response(&plan(), &response, 128 * 1024)
        .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let DescribeTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("expected results");
    };
    let (_, DescribeTopicResult::Described(description)) = outcomes[0].clone().into_parts() else {
        panic!("orders should be described");
    };
    assert_eq!(description.topic_id(), None);
}

#[test]
fn top_level_v13_error_remains_a_whole_operation_broker_rejection() {
    let mut response = MetadataResponse::default();
    response.error_code = -991;
    assert!(matches!(
        normalize_describe_topics_response(&plan(), &response, 1),
        Ok(DescribeTopicsInput::BrokerRejected { code })
            if code == NonZeroI16::new(-991)
                .unwrap_or_else(|| panic!("test code is nonzero"))
    ));
}

#[test]
fn duplicate_partition_is_rejected_without_collapsing_the_shape() {
    let mut response = MetadataResponse::default();
    response.topics = vec![
        described_topic("orders", [partition(0, 0), partition(0, 0)]),
        failed_topic("audit", 3),
    ];
    assert_eq!(
        normalize_describe_topics_response(&plan(), &response, 128 * 1024),
        Err(DescribeTopicsProtocolFailure::DuplicatePartition)
    );
}

#[test]
fn retained_result_must_fit_the_accepted_operation_reservation() {
    let mut response = MetadataResponse::default();
    response.topics = vec![
        described_topic("orders", [partition(0, 0)]),
        failed_topic("audit", 3),
    ];
    assert_eq!(
        normalize_describe_topics_response(&plan(), &response, 1),
        Err(DescribeTopicsProtocolFailure::RetainedBytes)
    );
}

#[test]
fn leader_isr_and_offline_membership_must_reference_declared_replicas() {
    for inconsistent in [3, 4, 5] {
        let mut partition = partition(0, 0);
        match inconsistent {
            3 => partition.leader_id = 9,
            4 => partition.isr_nodes = vec![9],
            5 => partition.offline_replicas = vec![9],
            _ => panic!("fixture selector is closed"),
        }
        let mut response = MetadataResponse::default();
        response.topics = vec![
            described_topic("orders", [partition]),
            failed_topic("audit", 3),
        ];
        assert_eq!(
            normalize_describe_topics_response(&plan(), &response, 128 * 1024),
            Err(DescribeTopicsProtocolFailure::ReplicaMembership)
        );
    }
}

fn plan() -> DescribeTopicsPlan {
    DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"))
}

fn failed_topic(name: &str, error_code: i16) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(name.into());
    topic.error_code = error_code;
    topic
}

fn described_topic<const N: usize>(
    name: &str,
    partitions: [MetadataResponsePartition; N],
) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(name.into());
    topic.partitions = Vec::from(partitions);
    topic
}

fn partition(index: i32, error_code: i16) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.error_code = error_code;
    partition.leader_id = -1;
    partition.leader_epoch = -1;
    partition.replica_nodes = vec![1, 2];
    partition.isr_nodes = vec![1];
    partition.offline_replicas = vec![2];
    partition
}
