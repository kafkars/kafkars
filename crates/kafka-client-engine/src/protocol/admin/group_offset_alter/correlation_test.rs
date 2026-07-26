//! Charged `OffsetCommit` sort-and-merge correlation scenarios.

use kafka_wire::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};

use super::{
    GroupOffsetAlterProtocolFailure, OffsetCommitTargetRef, correlation::correlate_response,
};

#[test]
fn sort_merge_restores_interleaved_caller_order() {
    let targets = [target("z", 4), target("a", 2), target("z", 1)];
    let topics = vec![
        topic("a", vec![partition(2)]),
        topic("z", vec![partition(1), partition(4)]),
    ];
    let entries = correlate_response(&targets, &topics, targets.len())
        .unwrap_or_else(|error| panic!("correlated response: {error:?}"));
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.topic(), entry.partition()))
            .collect::<Vec<_>>(),
        [("z", 4), ("a", 2), ("z", 1)]
    );
}

#[test]
fn duplicate_caller_target_is_rejected_before_merge() {
    let targets = [target("orders", 2), target("orders", 2)];
    let topics = vec![topic("orders", vec![partition(2), partition(3)])];
    assert_eq!(
        correlate_response(&targets, &topics, targets.len()).err(),
        Some(GroupOffsetAlterProtocolFailure::DuplicateTarget { actual: 2 })
    );
}

fn target(topic: &str, partition: i32) -> OffsetCommitTargetRef<'_> {
    OffsetCommitTargetRef::new(topic, partition, 1, None, None)
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
