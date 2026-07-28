//! Generated reassignment request construction scenarios.

use super::{AlterPartitionReassignmentRef, alter_partition_reassignments_request};

#[test]
fn request_groups_topics_without_losing_caller_replica_order_or_cancellation() {
    let changes = [
        AlterPartitionReassignmentRef::new("orders", 2, Some(&[4, 1, 7])),
        AlterPartitionReassignmentRef::new("audit", 0, None),
        AlterPartitionReassignmentRef::new("orders", 0, Some(&[2, 3])),
    ];
    let request = alter_partition_reassignments_request(&changes, true, 91, usize::MAX)
        .unwrap_or_else(|error| panic!("request: {error}"));

    assert_eq!(request.timeout_ms, 91);
    assert!(request.allow_replication_factor_change);
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].name.as_str(), "audit");
    assert_eq!(request.topics[0].partitions[0].replicas, None);
    assert_eq!(request.topics[1].name.as_str(), "orders");
    assert_eq!(request.topics[1].partitions[0].partition_index, 2);
    assert_eq!(
        request.topics[1].partitions[0].replicas.as_deref(),
        Some(&[4, 1, 7][..])
    );
}

#[test]
fn request_preserves_explicit_replication_factor_policy() {
    let changes = [AlterPartitionReassignmentRef::new(
        "orders",
        0,
        Some(&[1, 2]),
    )];
    let request = alter_partition_reassignments_request(&changes, false, 91, usize::MAX)
        .unwrap_or_else(|error| panic!("request: {error}"));

    assert!(!request.allow_replication_factor_change);
}

#[test]
fn request_rejects_negative_timeout_and_insufficient_scratch() {
    let changes = [AlterPartitionReassignmentRef::new("orders", 0, Some(&[1]))];
    assert!(alter_partition_reassignments_request(&changes, true, -1, usize::MAX).is_err());
    assert!(alter_partition_reassignments_request(&changes, true, 10, 0).is_err());
}
