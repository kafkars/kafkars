//! Scenarios for validated partition increases.

use super::{CreatePartitionsPlan, CreatePartitionsPlanError, CreatePartitionsSpecification};

fn topic(name: &str, count: i32) -> CreatePartitionsSpecification {
    CreatePartitionsSpecification::new(name.to_owned(), count)
}

#[test]
fn plan_preserves_order_and_rejects_ambiguous_or_invalid_topics() {
    let plan = CreatePartitionsPlan::new(vec![topic("orders", 8), topic("audit", 4)], true)
        .unwrap_or_else(|error| panic!("valid CreatePartitions plan: {error}"));
    assert_eq!(plan.topics()[0].topic(), "orders");
    assert_eq!(plan.topics()[0].total_count(), 8);
    assert_eq!(plan.topics()[0].replica_assignments(), None);
    assert!(plan.validate_only());
    assert_eq!(
        CreatePartitionsPlan::new(Vec::new(), false),
        Err(CreatePartitionsPlanError::EmptyBatch)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("", 2)], false),
        Err(CreatePartitionsPlanError::EmptyTopicName)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("orders", 0)], false),
        Err(CreatePartitionsPlanError::InvalidTotalCount)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("orders", 2), topic("orders", 3)], false),
        Err(CreatePartitionsPlanError::DuplicateTopic)
    );
}

#[test]
fn explicit_assignments_preserve_outer_and_broker_order() {
    let specification = CreatePartitionsSpecification::with_replica_assignments(
        "orders".to_owned(),
        5,
        vec![vec![3, 1, 2], vec![2, 3, 1]],
    );
    let plan = CreatePartitionsPlan::new(vec![specification], false)
        .unwrap_or_else(|error| panic!("valid explicit assignments: {error}"));

    assert_eq!(
        plan.topics()[0].replica_assignments(),
        Some([vec![3, 1, 2], vec![2, 3, 1]].as_slice())
    );
}

#[test]
fn explicit_assignments_reject_empty_negative_or_duplicate_brokers() {
    for (assignments, expected) in [
        (
            Vec::new(),
            CreatePartitionsPlanError::EmptyReplicaAssignments,
        ),
        (
            vec![Vec::new()],
            CreatePartitionsPlanError::EmptyReplicaAssignment,
        ),
        (
            vec![vec![1, -1]],
            CreatePartitionsPlanError::InvalidReplicaBrokerId,
        ),
        (
            vec![vec![1, 2, 1]],
            CreatePartitionsPlanError::DuplicateReplicaBrokerId,
        ),
    ] {
        assert_eq!(
            CreatePartitionsPlan::new(
                vec![CreatePartitionsSpecification::with_replica_assignments(
                    "orders".to_owned(),
                    5,
                    assignments,
                )],
                false,
            ),
            Err(expected)
        );
    }
}
