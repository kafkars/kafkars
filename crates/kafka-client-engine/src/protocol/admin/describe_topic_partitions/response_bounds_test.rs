//! Hostile topic, partition, broker-reference, and text count scenarios.

use kafka_client_core::{
    DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS,
    DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES, DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS,
};
use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
};

use super::{DescribeTopicPartitionsProtocolFailure, normalize_describe_topic_partitions_response};

const LIMIT: usize = 16 * 1024 * 1024;

#[test]
fn hostile_collection_counts_fail_before_materialization() {
    let mut response = DescribeTopicPartitionsResponse::default();
    response.topics = vec![
        DescribeTopicPartitionsResponseTopic::default();
        DESCRIBE_TOPIC_PARTITIONS_MAX_TOPICS + 1
    ];
    assert!(matches!(
        normalize(&response),
        Err(DescribeTopicPartitionsProtocolFailure::TooManyTopics { .. })
    ));

    let mut topic = named_topic("orders");
    topic.partitions =
        vec![valid_partition(); DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_PARTITIONS as usize + 1];
    response.topics = vec![topic];
    assert!(matches!(
        normalize(&response),
        Err(DescribeTopicPartitionsProtocolFailure::TooManyPartitions { .. })
    ));

    let mut partition = valid_partition();
    partition.replica_nodes = vec![0; DESCRIBE_TOPIC_PARTITIONS_MAX_BROKER_REFERENCES + 1];
    let mut topic = named_topic("orders");
    topic.partitions = vec![partition];
    response.topics = vec![topic];
    assert!(matches!(
        normalize(&response),
        Err(DescribeTopicPartitionsProtocolFailure::TooManyBrokerReferences { .. })
    ));
}

#[test]
fn aggregate_response_topic_text_is_bounded() {
    let one_name = i16::MAX as usize;
    let topic_count = DESCRIBE_TOPIC_PARTITIONS_MAX_RESPONSE_TOPIC_BYTES / one_name + 1;
    let mut response = DescribeTopicPartitionsResponse::default();
    response.topics = (0..topic_count)
        .map(|index| {
            let suffix = format!("{index:04}");
            named_topic(&format!("{}{suffix}", "x".repeat(one_name - suffix.len())))
        })
        .collect();
    assert!(matches!(
        normalize(&response),
        Err(DescribeTopicPartitionsProtocolFailure::ResponseTopicBytesExceeded { .. })
    ));
}

fn normalize(
    response: &DescribeTopicPartitionsResponse,
) -> Result<super::NormalizedDescribeTopicPartitionsResponse, DescribeTopicPartitionsProtocolFailure>
{
    normalize_describe_topic_partitions_response(0, response, LIMIT)
}

fn named_topic(name: &str) -> DescribeTopicPartitionsResponseTopic {
    let mut topic = DescribeTopicPartitionsResponseTopic::default();
    topic.name = Some(name.into());
    topic
}

fn valid_partition() -> DescribeTopicPartitionsResponsePartition {
    let mut partition = DescribeTopicPartitionsResponsePartition::default();
    partition.leader_id = -1;
    partition.leader_epoch = -1;
    partition
}
