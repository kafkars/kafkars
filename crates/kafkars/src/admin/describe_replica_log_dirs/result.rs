//! Caller-ordered selected-replica results with throttle observation.

use std::time::Duration;

use super::{super::BatchResult, ReplicaLogDirInfo};
use crate::admin::TopicPartitionReplica;

/// Fully settled log-directory descriptions for selected replicas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeReplicaLogDirsResult {
    throttle_time: Duration,
    replicas: BatchResult<TopicPartitionReplica, ReplicaLogDirInfo>,
}

impl DescribeReplicaLogDirsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        replicas: BatchResult<TopicPartitionReplica, ReplicaLogDirInfo>,
    ) -> Self {
        Self {
            throttle_time,
            replicas,
        }
    }

    /// Returns the maximum nonnegative throttle observed across broker calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-replica outcomes in original caller order.
    pub const fn replicas(&self) -> &BatchResult<TopicPartitionReplica, ReplicaLogDirInfo> {
        &self.replicas
    }

    /// Consumes this result into caller-ordered per-replica outcomes.
    pub fn into_replicas(self) -> BatchResult<TopicPartitionReplica, ReplicaLogDirInfo> {
        self.replicas
    }
}
