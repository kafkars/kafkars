//! Scenarios for bounded caller-ordered API-91 plans.

#![expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "fixture indices are bounded far below the protocol integer limit"
)]

use super::{
    ALTER_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES, AlterShareGroupOffset,
    AlterShareGroupOffsetsPlan, AlterShareGroupOffsetsPlanError,
};

#[test]
fn valid_plan_preserves_group_order_and_offsets() {
    let changes = vec![change("orders", 1, 42), change("audit", 0, 7)];
    let plan = AlterShareGroupOffsetsPlan::new("share-workers".to_owned(), changes.clone())
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "share-workers");
    assert_eq!(plan.changes(), changes);
    assert_eq!(plan.changes()[0].topic(), "orders");
    assert_eq!(plan.changes()[0].partition(), 1);
    assert_eq!(plan.changes()[0].start_offset(), 42);
}

#[test]
fn group_topic_partition_and_offset_bounds_are_explicit() {
    for (result, expected) in [
        (
            AlterShareGroupOffsetsPlan::new(String::new(), vec![change("a", 0, 0)]),
            AlterShareGroupOffsetsPlanError::EmptyGroupId,
        ),
        (
            AlterShareGroupOffsetsPlan::new(
                "g".repeat(ALTER_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES + 1),
                vec![change("a", 0, 0)],
            ),
            AlterShareGroupOffsetsPlanError::GroupIdTooLong,
        ),
        (
            AlterShareGroupOffsetsPlan::new("g".to_owned(), Vec::new()),
            AlterShareGroupOffsetsPlanError::EmptyAlterationBatch,
        ),
        (
            AlterShareGroupOffsetsPlan::new("g".to_owned(), vec![change("", 0, 0)]),
            AlterShareGroupOffsetsPlanError::EmptyTopicName,
        ),
        (
            AlterShareGroupOffsetsPlan::new(
                "g".to_owned(),
                vec![change(
                    &"t".repeat(ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES + 1),
                    0,
                    0,
                )],
            ),
            AlterShareGroupOffsetsPlanError::TopicNameTooLong,
        ),
        (
            AlterShareGroupOffsetsPlan::new("g".to_owned(), vec![change("a", -1, 0)]),
            AlterShareGroupOffsetsPlanError::NegativePartition,
        ),
        (
            AlterShareGroupOffsetsPlan::new("g".to_owned(), vec![change("a", 0, -1)]),
            AlterShareGroupOffsetsPlanError::NegativeStartOffset,
        ),
    ] {
        assert_eq!(result, Err(expected));
    }
}

#[test]
fn duplicate_and_count_bounds_reject_before_machine_construction() {
    assert_eq!(
        AlterShareGroupOffsetsPlan::new("g".to_owned(), vec![change("a", 1, 2), change("a", 1, 3)],),
        Err(AlterShareGroupOffsetsPlanError::DuplicateTopicPartition)
    );
    let too_many = (0..=ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS)
        .map(|partition| change("a", partition as i32, 0))
        .collect();
    assert_eq!(
        AlterShareGroupOffsetsPlan::new("g".to_owned(), too_many),
        Err(AlterShareGroupOffsetsPlanError::TooManyPartitions)
    );
}

fn change(topic: &str, partition: i32, start_offset: i64) -> AlterShareGroupOffset {
    AlterShareGroupOffset::new(topic.to_owned(), partition, start_offset)
}
