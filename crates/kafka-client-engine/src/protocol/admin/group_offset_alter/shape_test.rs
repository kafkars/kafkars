//! Allocation-free hostile `OffsetCommit` response-shape scenarios.

use kafka_wire::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};

use super::{GroupOffsetAlterProtocolFailure, shape::validate_response_shape};

#[test]
fn empty_topics_partitions_and_negative_indices_are_rejected() {
    assert_eq!(
        validate_response_shape(&[topic("", vec![partition(0)])], 1),
        Err(GroupOffsetAlterProtocolFailure::EmptyTopic)
    );
    assert_eq!(
        validate_response_shape(&[topic("orders", Vec::new())], 1),
        Err(GroupOffsetAlterProtocolFailure::EmptyTopicPartitions)
    );
    assert_eq!(
        validate_response_shape(&[topic("orders", vec![partition(-1)])], 1),
        Err(GroupOffsetAlterProtocolFailure::NegativePartition { actual: -1 })
    );
}

#[test]
fn excess_partition_count_stops_at_the_first_unadmitted_entry() {
    let topics = [topic(
        "orders",
        vec![partition(0), partition(1), partition(2)],
    )];
    assert_eq!(
        validate_response_shape(&topics, 2),
        Err(GroupOffsetAlterProtocolFailure::PartitionCount {
            expected: 2,
            actual: 3,
        })
    );
}

fn partition(partition_index: i32) -> OffsetCommitResponsePartition {
    let mut partition = OffsetCommitResponsePartition::default();
    partition.partition_index = partition_index;
    partition
}

fn topic(name: &str, partitions: Vec<OffsetCommitResponsePartition>) -> OffsetCommitResponseTopic {
    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}
