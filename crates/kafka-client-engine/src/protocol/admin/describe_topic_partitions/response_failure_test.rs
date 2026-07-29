//! Version, throttle, scalar, cursor, duplicate, and capacity failures.

use kafka_wire::{
    DescribeTopicPartitionsResponse,
    describe_topic_partitions_response::{
        Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
};

use super::{DescribeTopicPartitionsProtocolFailure, normalize_describe_topic_partitions_response};

const LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn selected_version_and_throttle_are_strict() {
    let response = DescribeTopicPartitionsResponse::default();
    for actual in [-1, 1, i16::MAX] {
        assert_eq!(
            normalize(actual, &response),
            Err(DescribeTopicPartitionsProtocolFailure::UnsupportedApiVersion { actual })
        );
    }
    let mut response = response;
    response.throttle_time_ms = -1;
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn invalid_scalars_and_cursor_shapes_are_rejected() {
    let mut response = response_with_partition();
    response.topics[0].partitions[0].partition_index = -1;
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::NegativePartition { actual: -1 })
    );
    response.topics[0].partitions[0].partition_index = 0;
    response.topics[0].partitions[0].leader_id = -2;
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::InvalidLeaderId { actual: -2 })
    );
    response.topics[0].partitions[0].leader_id = 1;
    response.topics[0].partitions[0].leader_epoch = -2;
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::InvalidLeaderEpoch { actual: -2 })
    );

    let mut response = DescribeTopicPartitionsResponse::default();
    let mut cursor = Cursor::default();
    cursor.topic_name = "orders".into();
    cursor.partition_index = -1;
    response.next_cursor = Some(cursor);
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::NegativeCursorPartition { actual: -1 })
    );
}

#[test]
fn protocol_local_duplicates_and_negative_brokers_are_rejected() {
    let mut response = response_with_partition();
    let duplicate_topic = response.topics[0].clone();
    response.topics.push(duplicate_topic);
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::DuplicateTopicName)
    );

    let mut response = response_with_partition();
    let duplicate_partition = response.topics[0].partitions[0].clone();
    response.topics[0].partitions.push(duplicate_partition);
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::DuplicatePartition { actual: 0 })
    );

    let mut response = response_with_partition();
    response.topics[0].partitions[0].replica_nodes = vec![2, 2];
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::DuplicateBrokerId {
            field: "replica_nodes",
            actual: 2,
        })
    );
    response.topics[0].partitions[0].replica_nodes = vec![-7];
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::NegativeBrokerId {
            field: "replica_nodes",
            actual: -7,
        })
    );
}

#[test]
fn present_empty_names_and_insufficient_capacity_are_rejected() {
    let mut response = DescribeTopicPartitionsResponse::default();
    let mut topic = DescribeTopicPartitionsResponseTopic::default();
    topic.name = Some("".into());
    response.topics = vec![topic];
    assert_eq!(
        normalize(0, &response),
        Err(DescribeTopicPartitionsProtocolFailure::EmptyTopicName)
    );

    let response = response_with_partition();
    assert!(matches!(
        normalize_describe_topic_partitions_response(0, &response, 0),
        Err(DescribeTopicPartitionsProtocolFailure::RetainedBytes { .. })
    ));
}

fn normalize(
    version: i16,
    response: &DescribeTopicPartitionsResponse,
) -> Result<super::NormalizedDescribeTopicPartitionsResponse, DescribeTopicPartitionsProtocolFailure>
{
    normalize_describe_topic_partitions_response(version, response, LIMIT)
}

fn response_with_partition() -> DescribeTopicPartitionsResponse {
    let mut partition = DescribeTopicPartitionsResponsePartition::default();
    partition.leader_id = -1;
    partition.leader_epoch = -1;
    partition.replica_nodes = vec![2, 9];
    let mut topic = DescribeTopicPartitionsResponseTopic::default();
    topic.name = Some("orders".into());
    topic.partitions = vec![partition];
    let mut response = DescribeTopicPartitionsResponse::default();
    response.topics = vec![topic];
    response
}
