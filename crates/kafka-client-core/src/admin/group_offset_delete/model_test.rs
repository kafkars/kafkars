//! Scenarios for consumer-group offset deletion intent validation.

use super::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsPlanError,
};

#[test]
fn plan_preserves_explicit_group_and_caller_partition_order() {
    let plan = DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![target("orders", 2), target("audit", 0)],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.targets()[0], target("orders", 2));
    assert_eq!(plan.targets()[1], target("audit", 0));
}

#[test]
fn plan_rejects_empty_or_unrepresentable_group_identity() {
    assert_eq!(
        DeleteConsumerGroupOffsetsPlan::new(String::new(), vec![target("orders", 0)]),
        Err(DeleteConsumerGroupOffsetsPlanError::EmptyGroupId)
    );
    assert_eq!(
        DeleteConsumerGroupOffsetsPlan::new(
            "g".repeat(i16::MAX as usize + 1),
            vec![target("orders", 0)],
        ),
        Err(DeleteConsumerGroupOffsetsPlanError::GroupIdTooLong)
    );
}

#[test]
fn plan_rejects_empty_invalid_or_duplicate_topic_partitions() {
    for (targets, expected) in [
        (
            Vec::new(),
            DeleteConsumerGroupOffsetsPlanError::EmptyTargetBatch,
        ),
        (
            vec![target("", 0)],
            DeleteConsumerGroupOffsetsPlanError::EmptyTopicName,
        ),
        (
            vec![target(&"t".repeat(i16::MAX as usize + 1), 0)],
            DeleteConsumerGroupOffsetsPlanError::TopicNameTooLong,
        ),
        (
            vec![target("orders", -1)],
            DeleteConsumerGroupOffsetsPlanError::NegativePartition,
        ),
        (
            vec![target("orders", 1), target("orders", 1)],
            DeleteConsumerGroupOffsetsPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(
            DeleteConsumerGroupOffsetsPlan::new("payments".to_owned(), targets),
            Err(expected)
        );
    }
}

fn target(topic: &str, partition: i32) -> DeleteConsumerGroupOffsetTarget {
    DeleteConsumerGroupOffsetTarget::new(topic.to_owned(), partition)
}
