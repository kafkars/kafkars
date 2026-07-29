//! Scenarios for consumer-group offset alteration intent validation.

use super::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsPlanError,
};

#[test]
fn plan_preserves_group_caller_order_offsets_epochs_and_nullable_metadata() {
    let plan = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            target("orders", 2, 91, Some(7), Some(String::new())),
            target("audit", 0, 13, None, None),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].next_offset(), 91);
    assert_eq!(plan.targets()[0].leader_epoch(), Some(7));
    assert_eq!(plan.targets()[0].metadata(), Some(""));
    assert_eq!(plan.targets()[1].metadata(), None);
    assert_eq!(plan.retention_time_ms(), None);
    assert!(plan.requires_leader_epoch());
}

#[test]
fn plan_preserves_explicit_nonnegative_retention_milliseconds() {
    let zero = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![plain_target("orders", 0, 1)],
    )
    .and_then(|plan| plan.with_retention_time_ms(0))
    .unwrap_or_else(|error| panic!("zero retention plan: {error}"));
    let positive = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![plain_target("orders", 0, 1)],
    )
    .and_then(|plan| plan.with_retention_time_ms(i64::MAX))
    .unwrap_or_else(|error| panic!("maximum retention plan: {error}"));

    assert_eq!(zero.retention_time_ms(), Some(0));
    assert_eq!(positive.retention_time_ms(), Some(i64::MAX));
}

#[test]
fn plan_rejects_negative_retention_and_retention_with_leader_epoch() {
    let plain = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![plain_target("orders", 0, 1)],
    )
    .unwrap_or_else(|error| panic!("plain plan: {error}"));
    assert_eq!(
        plain.with_retention_time_ms(-1),
        Err(AlterConsumerGroupOffsetsPlanError::NegativeRetentionTime)
    );

    let epoch = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![target("orders", 0, 1, Some(7), None)],
    )
    .unwrap_or_else(|error| panic!("leader-epoch plan: {error}"));
    assert_eq!(
        epoch.with_retention_time_ms(1),
        Err(AlterConsumerGroupOffsetsPlanError::RetentionTimeWithLeaderEpoch)
    );
}

#[test]
fn plan_rejects_empty_or_unrepresentable_group_identity() {
    assert_eq!(
        AlterConsumerGroupOffsetsPlan::new(String::new(), vec![plain_target("orders", 0, 1)]),
        Err(AlterConsumerGroupOffsetsPlanError::EmptyGroupId)
    );
    assert_eq!(
        AlterConsumerGroupOffsetsPlan::new(
            "g".repeat(i16::MAX as usize + 1),
            vec![plain_target("orders", 0, 1)],
        ),
        Err(AlterConsumerGroupOffsetsPlanError::GroupIdTooLong)
    );
}

#[test]
fn plan_rejects_invalid_offset_epoch_and_metadata_facts() {
    for (target, expected) in [
        (
            plain_target("", 0, 1),
            AlterConsumerGroupOffsetsPlanError::EmptyTopicName,
        ),
        (
            plain_target(&"t".repeat(i16::MAX as usize + 1), 0, 1),
            AlterConsumerGroupOffsetsPlanError::TopicNameTooLong,
        ),
        (
            plain_target("orders", -1, 1),
            AlterConsumerGroupOffsetsPlanError::NegativePartition,
        ),
        (
            plain_target("orders", 0, -1),
            AlterConsumerGroupOffsetsPlanError::NegativeNextOffset,
        ),
        (
            target("orders", 0, 1, Some(-1), None),
            AlterConsumerGroupOffsetsPlanError::NegativeLeaderEpoch,
        ),
        (
            target(
                "orders",
                0,
                1,
                None,
                Some("m".repeat(i16::MAX as usize + 1)),
            ),
            AlterConsumerGroupOffsetsPlanError::MetadataTooLong,
        ),
    ] {
        assert_eq!(
            AlterConsumerGroupOffsetsPlan::new("payments".to_owned(), vec![target]),
            Err(expected)
        );
    }
}

#[test]
fn plan_rejects_empty_and_duplicate_target_batches() {
    assert_eq!(
        AlterConsumerGroupOffsetsPlan::new("payments".to_owned(), Vec::new()),
        Err(AlterConsumerGroupOffsetsPlanError::EmptyTargetBatch)
    );
    assert_eq!(
        AlterConsumerGroupOffsetsPlan::new(
            "payments".to_owned(),
            vec![
                plain_target("orders", 1, 4),
                target("orders", 1, 9, Some(3), Some("later".to_owned())),
            ],
        ),
        Err(AlterConsumerGroupOffsetsPlanError::DuplicateTopicPartition)
    );
}

fn plain_target(topic: &str, partition: i32, next_offset: i64) -> AlterConsumerGroupOffsetTarget {
    target(topic, partition, next_offset, None, None)
}

fn target(
    topic: &str,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
) -> AlterConsumerGroupOffsetTarget {
    AlterConsumerGroupOffsetTarget::new(
        topic.to_owned(),
        partition,
        next_offset,
        leader_epoch,
        metadata,
    )
}
