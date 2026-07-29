//! Replica validation and first-occurrence broker grouping scenarios.

use super::{
    DESCRIBE_REPLICA_LOG_DIRS_MAX_TOPIC_BYTES, DescribeReplicaLogDirsPlan,
    DescribeReplicaLogDirsPlanError, DescribeReplicaLogDirsReplica,
};

#[test]
fn plan_preserves_caller_order_and_groups_brokers_by_first_occurrence() {
    let plan = DescribeReplicaLogDirsPlan::new(vec![
        replica("orders", 0, 7),
        replica("audit", 1, 2),
        replica("orders", 1, 7),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.broker_ids(), &[7, 2]);
    assert_eq!(
        plan.replicas(),
        [
            replica("orders", 0, 7),
            replica("audit", 1, 2),
            replica("orders", 1, 7),
        ]
    );
    assert_eq!(
        plan.replicas_for_broker(7)
            .map(DescribeReplicaLogDirsReplica::partition)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn plan_rejects_every_invalid_identity_and_exact_duplicates() {
    for (replicas, expected) in [
        (
            Vec::new(),
            DescribeReplicaLogDirsPlanError::EmptyReplicaBatch,
        ),
        (
            vec![replica("", 0, 0)],
            DescribeReplicaLogDirsPlanError::EmptyTopic,
        ),
        (
            vec![replica(
                &"x".repeat(DESCRIBE_REPLICA_LOG_DIRS_MAX_TOPIC_BYTES + 1),
                0,
                0,
            )],
            DescribeReplicaLogDirsPlanError::TopicTooLong,
        ),
        (
            vec![replica("orders", -1, 0)],
            DescribeReplicaLogDirsPlanError::NegativePartition,
        ),
        (
            vec![replica("orders", 0, -1)],
            DescribeReplicaLogDirsPlanError::NegativeBrokerId,
        ),
        (
            vec![replica("orders", 0, 1), replica("orders", 0, 1)],
            DescribeReplicaLogDirsPlanError::DuplicateReplica,
        ),
    ] {
        assert_eq!(DescribeReplicaLogDirsPlan::new(replicas), Err(expected));
    }
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}
