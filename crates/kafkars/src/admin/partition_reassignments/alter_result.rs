//! Public reassignment alteration result with Kafka throttle observation.

use std::time::Duration;

use crate::{TopicPartition, admin::BatchResult};

/// Successful deterministic partition-reassignment alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterPartitionReassignmentsResult {
    throttle_time: Duration,
    partitions: BatchResult<TopicPartition, ()>,
}

impl AlterPartitionReassignmentsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        partitions: BatchResult<TopicPartition, ()>,
    ) -> Self {
        Self {
            throttle_time,
            partitions,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-partition outcomes in original caller order.
    pub const fn partitions(&self) -> &BatchResult<TopicPartition, ()> {
        &self.partitions
    }

    /// Consumes this result into caller-ordered per-partition outcomes.
    pub fn into_partitions(self) -> BatchResult<TopicPartition, ()> {
        self.partitions
    }
}
