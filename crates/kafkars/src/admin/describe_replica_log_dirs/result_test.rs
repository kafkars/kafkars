//! Public selected-replica result ordering and throttle coverage.

use std::time::Duration;

use crate::admin::{BatchResult, TopicPartitionReplica};

use super::{DescribeReplicaLogDirsResult, ReplicaLogDirInfo};

#[test]
fn result_retains_throttle_and_caller_order() {
    let result = DescribeReplicaLogDirsResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            (
                TopicPartitionReplica::new("orders", 0, 8),
                Ok(ReplicaLogDirInfo::new(None, None)),
            ),
            (
                TopicPartitionReplica::new("audit", 2, 3),
                Ok(ReplicaLogDirInfo::new(None, None)),
            ),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.replicas().entries()[0].0.topic(), "orders");
    assert_eq!(result.replicas().entries()[1].0.broker_id(), 3);
    assert_eq!(result.into_replicas().entries().len(), 2);
}
