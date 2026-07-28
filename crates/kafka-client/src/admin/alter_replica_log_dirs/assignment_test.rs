//! Explicit replica target-path assignment tests.

use super::{ReplicaLogDirAssignment, TopicPartitionReplica};

#[test]
fn assignment_keeps_replica_identity_separate_from_target_path() {
    let assignment =
        ReplicaLogDirAssignment::new(TopicPartitionReplica::new("orders", 3, 7), "/kafka-fast");

    assert_eq!(assignment.replica().topic(), "orders");
    assert_eq!(assignment.replica().partition(), 3);
    assert_eq!(assignment.replica().broker_id(), 7);
    assert_eq!(assignment.target_path(), "/kafka-fast");
}
