//! Validation scenarios for deterministic Admin `DeleteRecords` intent.

use super::{DeleteRecordsPlan, DeleteRecordsPlanError, DeleteRecordsTarget};

#[test]
fn plan_preserves_order_and_accepts_high_watermark_sentinel() {
    let plan = DeleteRecordsPlan::new(vec![target("orders", 2, 91), target("audit", 0, -1)])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].before_offset(), 91);
    assert_eq!(plan.targets()[1].before_offset(), -1);
}

#[test]
fn plan_rejects_invalid_and_duplicate_targets() {
    for (targets, expected) in [
        (Vec::new(), DeleteRecordsPlanError::EmptyTargetBatch),
        (
            vec![target("", 0, 1)],
            DeleteRecordsPlanError::EmptyTopicName,
        ),
        (
            vec![target("orders", -1, 1)],
            DeleteRecordsPlanError::NegativePartition,
        ),
        (
            vec![target("orders", 0, -2)],
            DeleteRecordsPlanError::InvalidOffset,
        ),
        (
            vec![target("orders", 0, 1), target("orders", 0, 2)],
            DeleteRecordsPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(DeleteRecordsPlan::new(targets), Err(expected));
    }
}

fn target(topic: &str, partition: i32, offset: i64) -> DeleteRecordsTarget {
    DeleteRecordsTarget::new(topic.to_owned(), partition, offset)
}
