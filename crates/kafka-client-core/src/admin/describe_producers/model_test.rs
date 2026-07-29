//! Scenarios for bounded active-producer request validation.

use super::{
    AdminDescribeProducerTarget, AdminDescribeProducersPlan, AdminDescribeProducersPlanError,
    DESCRIBE_PRODUCERS_MAX_TARGET_TOPIC_BYTES, DESCRIBE_PRODUCERS_MAX_TARGETS,
};

#[test]
fn plan_preserves_caller_order() {
    let plan =
        AdminDescribeProducersPlan::new(vec![target("orders", 2), target("audit", 0)], Some(7))
            .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.targets()[0].topic(), "orders");
    assert_eq!(plan.targets()[0].partition(), 2);
    assert_eq!(plan.targets()[1].topic(), "audit");
    assert_eq!(plan.targets()[1].partition(), 0);
    assert_eq!(plan.broker_id(), Some(7));
}

#[test]
fn plan_rejects_empty_invalid_and_duplicate_targets() {
    for (targets, expected) in [
        (
            Vec::new(),
            AdminDescribeProducersPlanError::EmptyTargetBatch,
        ),
        (
            vec![target("", 0)],
            AdminDescribeProducersPlanError::EmptyTopicName,
        ),
        (
            vec![target(&"t".repeat(250), 0)],
            AdminDescribeProducersPlanError::TopicNameTooLong,
        ),
        (
            vec![target("orders", -1)],
            AdminDescribeProducersPlanError::NegativePartition,
        ),
        (
            vec![target("orders", 0), target("orders", 0)],
            AdminDescribeProducersPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(
            AdminDescribeProducersPlan::new(targets, None),
            Err(expected)
        );
    }
}

#[test]
fn exact_broker_selection_is_optional_and_rejects_negative_ids() {
    let default = AdminDescribeProducersPlan::new(vec![target("orders", 0)], None)
        .unwrap_or_else(|error| panic!("valid default route: {error}"));
    assert_eq!(default.broker_id(), None);
    assert_eq!(
        AdminDescribeProducersPlan::new(vec![target("orders", 0)], Some(-1)),
        Err(AdminDescribeProducersPlanError::NegativeBrokerId)
    );
}

#[test]
fn plan_bounds_target_count_and_aggregate_topic_bytes() {
    let too_many = (0..=DESCRIBE_PRODUCERS_MAX_TARGETS)
        .map(|partition| target("t", partition as i32))
        .collect();
    assert_eq!(
        AdminDescribeProducersPlan::new(too_many, None),
        Err(AdminDescribeProducersPlanError::TooManyTargets)
    );

    let topic = "t".repeat(249);
    let count = DESCRIBE_PRODUCERS_MAX_TARGET_TOPIC_BYTES / topic.len() + 1;
    let too_many_bytes = (0..count)
        .map(|partition| target(&topic, partition as i32))
        .collect();
    assert_eq!(
        AdminDescribeProducersPlan::new(too_many_bytes, None),
        Err(AdminDescribeProducersPlanError::TargetTopicBytesExceeded)
    );
}

fn target(topic: &str, partition: i32) -> AdminDescribeProducerTarget {
    AdminDescribeProducerTarget::new(topic.to_owned(), partition)
}
