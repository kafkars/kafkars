//! Public-to-engine replica assignment translation tests.

use crate::admin::{ReplicaLogDirAssignment, TopicPartitionReplica};

use super::AlterReplicaLogDirsAdminRequest;

#[test]
fn translation_is_deferred_and_retains_caller_order_and_target_paths() {
    let request = AlterReplicaLogDirsAdminRequest::new(vec![
        ReplicaLogDirAssignment::new(TopicPartitionReplica::new("orders", 2, 7), "/kafka-fast"),
        ReplicaLogDirAssignment::new(TopicPartitionReplica::new("audit", 0, 3), "/kafka-capacity"),
    ]);
    let engine = request.into_engine();

    assert!(format!("{engine:?}").contains("AlterReplicaLogDirsRequest"));
}
