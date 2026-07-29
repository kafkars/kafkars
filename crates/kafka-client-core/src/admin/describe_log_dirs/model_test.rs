//! Validation scenarios for caller-selected brokers and topic-partitions.

use super::{
    ADMIN_DESCRIBE_LOG_DIRS_MAX_PARTITIONS, ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPIC_BYTES,
    ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPICS, AdminDescribeLogDirsPartition, AdminDescribeLogDirsPlan,
    AdminDescribeLogDirsPlanError, AdminDescribeLogDirsSelection,
};

#[test]
fn all_topic_plan_preserves_nonempty_unique_broker_order() {
    let plan = AdminDescribeLogDirsPlan::new(vec![9, 2, 7])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.broker_ids(), &[9, 2, 7]);
    assert_eq!(plan.selection(), &AdminDescribeLogDirsSelection::AllTopics);
}

#[test]
fn selected_plan_preserves_flat_caller_order() {
    let plan = AdminDescribeLogDirsPlan::selected(
        vec![9, 2],
        vec![
            partition("orders", 2),
            partition("audit", 0),
            partition("orders", 1),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    let Some(partitions) = plan.selection().selected_partitions() else {
        panic!("explicit selection expected");
    };
    assert_eq!(
        partitions
            .iter()
            .map(|partition| (partition.topic(), partition.partition()))
            .collect::<Vec<_>>(),
        vec![("orders", 2), ("audit", 0), ("orders", 1)]
    );
}

#[test]
fn plan_rejects_empty_negative_and_duplicate_brokers() {
    for (broker_ids, expected) in [
        (Vec::new(), AdminDescribeLogDirsPlanError::EmptyBrokerBatch),
        (vec![3, -1], AdminDescribeLogDirsPlanError::NegativeBrokerId),
        (
            vec![3, 8, 3],
            AdminDescribeLogDirsPlanError::DuplicateBrokerId,
        ),
    ] {
        assert_eq!(AdminDescribeLogDirsPlan::new(broker_ids), Err(expected));
    }
}

#[test]
fn selected_plan_rejects_invalid_partition_identities() {
    for (partitions, expected) in [
        (Vec::new(), AdminDescribeLogDirsPlanError::EmptySelection),
        (
            vec![partition("", 0)],
            AdminDescribeLogDirsPlanError::EmptyTopic,
        ),
        (
            vec![partition(
                &"x".repeat(ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPIC_BYTES + 1),
                0,
            )],
            AdminDescribeLogDirsPlanError::TopicTooLong,
        ),
        (
            vec![partition("orders", -1)],
            AdminDescribeLogDirsPlanError::NegativePartition,
        ),
        (
            vec![partition("orders", 1), partition("orders", 1)],
            AdminDescribeLogDirsPlanError::DuplicatePartition,
        ),
    ] {
        assert_eq!(
            AdminDescribeLogDirsPlan::selected(vec![1], partitions),
            Err(expected)
        );
    }
}

#[test]
fn selected_plan_rejects_hostile_topic_and_partition_counts_before_admission() {
    let topics = (0..=ADMIN_DESCRIBE_LOG_DIRS_MAX_TOPICS)
        .map(|index| partition(&format!("topic-{index}"), 0))
        .collect();
    assert_eq!(
        AdminDescribeLogDirsPlan::selected(vec![1], topics),
        Err(AdminDescribeLogDirsPlanError::TooManyTopics)
    );

    let partitions = vec![
        AdminDescribeLogDirsPartition::new(String::new(), 0);
        ADMIN_DESCRIBE_LOG_DIRS_MAX_PARTITIONS + 1
    ];
    assert_eq!(
        AdminDescribeLogDirsPlan::selected(vec![1], partitions),
        Err(AdminDescribeLogDirsPlanError::TooManyPartitions)
    );
}

fn partition(topic: &str, partition: i32) -> AdminDescribeLogDirsPartition {
    AdminDescribeLogDirsPartition::new(topic.to_owned(), partition)
}
