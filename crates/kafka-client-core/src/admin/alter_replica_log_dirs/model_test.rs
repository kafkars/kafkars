//! Validation and deterministic grouping scenarios for assignment intent.

use super::{AlterReplicaLogDirAssignment, AlterReplicaLogDirsPlan, AlterReplicaLogDirsPlanError};

#[test]
fn plan_preserves_caller_order_and_first_appearance_broker_order() {
    let plan = AlterReplicaLogDirsPlan::new(vec![
        assignment(9, "orders", 0, "/b"),
        assignment(2, "audit", 1, "/a"),
        assignment(9, "orders", 2, "/c"),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.broker_ids(), &[9, 2]);
    assert_eq!(plan.assignments()[0].log_dir(), "/b");
    assert_eq!(plan.assignments()[1].topic(), "audit");
    assert_eq!(plan.assignments()[2].partition(), 2);
}

#[test]
fn plan_rejects_invalid_and_duplicate_replica_assignments() {
    for (assignments, expected) in [
        (
            Vec::new(),
            AlterReplicaLogDirsPlanError::EmptyAssignmentBatch,
        ),
        (
            vec![assignment(-1, "orders", 0, "/a")],
            AlterReplicaLogDirsPlanError::NegativeBrokerId,
        ),
        (
            vec![assignment(1, "", 0, "/a")],
            AlterReplicaLogDirsPlanError::EmptyTopicName,
        ),
        (
            vec![assignment(1, "orders", -1, "/a")],
            AlterReplicaLogDirsPlanError::NegativePartition,
        ),
        (
            vec![assignment(1, "orders", 0, "")],
            AlterReplicaLogDirsPlanError::EmptyLogDir,
        ),
        (
            vec![
                assignment(1, "orders", 0, "/a"),
                assignment(1, "orders", 0, "/b"),
            ],
            AlterReplicaLogDirsPlanError::DuplicateReplica,
        ),
    ] {
        assert_eq!(AlterReplicaLogDirsPlan::new(assignments), Err(expected));
    }
}

fn assignment(
    broker_id: i32,
    topic: &str,
    partition: i32,
    log_dir: &str,
) -> AlterReplicaLogDirAssignment {
    AlterReplicaLogDirAssignment::new(broker_id, topic.to_owned(), partition, log_dir.to_owned())
}
