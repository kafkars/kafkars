//! Validation scenarios for partition-reassignment alteration intent.

use super::{
    AlterPartitionReassignment, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsPlanError, PartitionReassignmentTarget,
};

#[test]
fn plan_preserves_caller_order_replica_order_and_cancellation() {
    let plan = AlterPartitionReassignmentsPlan::new(vec![
        replacement("orders", 2, vec![4, 1, 7]),
        cancellation("audit", 0),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.changes()[0].topic(), "orders");
    assert_eq!(plan.changes()[0].partition(), 2);
    assert_eq!(plan.changes()[0].target().replicas(), Some(&[4, 1, 7][..]));
    assert_eq!(plan.changes()[1].topic(), "audit");
    assert_eq!(plan.changes()[1].target().replicas(), None);
    assert!(plan.allow_replication_factor_change());
}

#[test]
fn replication_factor_policy_defaults_true_and_preserves_explicit_false() {
    let plan = AlterPartitionReassignmentsPlan::new(vec![replacement("orders", 0, vec![1, 2])])
        .unwrap_or_else(|error| panic!("valid plan: {error}"))
        .with_allow_replication_factor_change(false);

    assert!(!plan.allow_replication_factor_change());
}

#[test]
fn plan_rejects_invalid_change_identity_and_replacement() {
    for (changes, expected) in [
        (Vec::new(), AlterPartitionReassignmentsPlanError::EmptyBatch),
        (
            vec![replacement("", 0, vec![1])],
            AlterPartitionReassignmentsPlanError::EmptyTopicName,
        ),
        (
            vec![replacement("orders", -1, vec![1])],
            AlterPartitionReassignmentsPlanError::NegativePartition,
        ),
        (
            vec![replacement("orders", 0, Vec::new())],
            AlterPartitionReassignmentsPlanError::EmptyReplicaList,
        ),
        (
            vec![replacement("orders", 0, vec![-1])],
            AlterPartitionReassignmentsPlanError::NegativeBrokerId,
        ),
        (
            vec![replacement("orders", 0, vec![1, 1])],
            AlterPartitionReassignmentsPlanError::DuplicateBrokerId,
        ),
        (
            vec![replacement("orders", 0, vec![1]), cancellation("orders", 0)],
            AlterPartitionReassignmentsPlanError::DuplicateTopicPartition,
        ),
    ] {
        assert_eq!(AlterPartitionReassignmentsPlan::new(changes), Err(expected));
    }
}

#[test]
fn cancellation_needs_no_replica_placeholder() {
    let plan = AlterPartitionReassignmentsPlan::new(vec![cancellation("orders", 0)])
        .unwrap_or_else(|error| panic!("valid cancellation: {error}"));
    assert_eq!(
        plan.changes()[0].target(),
        &PartitionReassignmentTarget::Cancel
    );
}

fn replacement(topic: &str, partition: i32, replicas: Vec<i32>) -> AlterPartitionReassignment {
    AlterPartitionReassignment::new(
        topic.to_owned(),
        partition,
        PartitionReassignmentTarget::Replicas(replicas),
    )
}

fn cancellation(topic: &str, partition: i32) -> AlterPartitionReassignment {
    AlterPartitionReassignment::new(
        topic.to_owned(),
        partition,
        PartitionReassignmentTarget::Cancel,
    )
}
