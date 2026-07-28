//! Caller-ordered per-replica alteration result with throttle observation.

use std::time::Duration;

use super::{super::BatchResult, TopicPartitionReplica};

/// Fully settled replica log-directory assignments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterReplicaLogDirsResult {
    throttle_time: Duration,
    replicas: BatchResult<TopicPartitionReplica, ()>,
}

impl AlterReplicaLogDirsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        replicas: BatchResult<TopicPartitionReplica, ()>,
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

    /// Returns exact per-replica outcomes in original caller order.
    pub const fn replicas(&self) -> &BatchResult<TopicPartitionReplica, ()> {
        &self.replicas
    }

    /// Consumes this result into caller-ordered per-replica outcomes.
    pub fn into_replicas(self) -> BatchResult<TopicPartitionReplica, ()> {
        self.replicas
    }
}
