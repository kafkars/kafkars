//! Charged sort/merge response-correlation scenarios.

use kafka_wire::offset_delete_response::{
    OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};

use super::{
    GroupOffsetDeleteProtocolFailure, OffsetDeleteTargetRef, correlation::correlate_response,
};

#[test]
fn sort_merge_restores_interleaved_caller_order() {
    let targets = [
        OffsetDeleteTargetRef::new("z", 4),
        OffsetDeleteTargetRef::new("a", 2),
        OffsetDeleteTargetRef::new("z", 1),
    ];
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
    let targets = [
        OffsetDeleteTargetRef::new("orders", 2),
        OffsetDeleteTargetRef::new("orders", 2),
    ];
    let topics = vec![topic("orders", vec![partition(2), partition(3)])];
    assert_eq!(
        correlate_response(&targets, &topics, targets.len()).err(),
        Some(GroupOffsetDeleteProtocolFailure::DuplicateTarget { actual: 2 })
    );
}

fn partition(partition_index: i32) -> OffsetDeleteResponsePartition {
    let mut partition = OffsetDeleteResponsePartition::default();
    partition.partition_index = partition_index;
    partition
}

fn topic(name: &str, partitions: Vec<OffsetDeleteResponsePartition>) -> OffsetDeleteResponseTopic {
    let mut topic = OffsetDeleteResponseTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}
