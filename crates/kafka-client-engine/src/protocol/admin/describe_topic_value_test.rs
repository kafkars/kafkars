//! Single-topic generated Metadata value-normalization scenarios.

use kafka_client_core::DescribeTopicResult;
use kafka_wire::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};

use super::describe_topic_value::normalize_topic;

#[test]
fn success_derives_one_consistent_topic_name_and_internal_fact() {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("orders".into());
    topic.is_internal = true;
    topic.partitions = vec![partition(1), partition(0)];

    let outcome =
        normalize_topic("orders", &topic).unwrap_or_else(|error| panic!("valid topic: {error:?}"));
    assert_eq!(outcome.topic(), "orders");
    assert!(outcome.is_internal());
    let (_, internal, DescribeTopicResult::Described(description)) = outcome.into_parts() else {
        panic!("description expected");
    };
    assert!(internal);
    assert_eq!(description.name(), "orders");
    assert!(description.is_internal());
    assert_eq!(description.partitions()[0].partition_index(), 0);
}

#[test]
fn failure_preserves_internal_status_and_exact_signed_broker_code() {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some("consumer_offsets".into());
    topic.is_internal = true;
    topic.error_code = -731;

    let outcome = normalize_topic("consumer_offsets", &topic)
        .unwrap_or_else(|error| panic!("valid broker failure: {error:?}"));
    let (name, internal, DescribeTopicResult::Failed(error)) = outcome.into_parts() else {
        panic!("broker failure expected");
    };
    assert_eq!(name, "consumer_offsets");
    assert!(internal);
    assert_eq!(error.code(), -731);
}

fn partition(index: i32) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.leader_id = -1;
    partition.leader_epoch = -1;
    partition.replica_nodes = vec![1];
    partition.isr_nodes = vec![1];
    partition
}
