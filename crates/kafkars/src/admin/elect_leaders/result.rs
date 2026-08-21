//! Public leader election alteration result with Kafka throttle observation.

use std::time::Duration;

use crate::{TopicPartition, admin::BatchResult};

/// Successful deterministic leader-election alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectLeadersResult {
    throttle_time: Duration,
    partitions: BatchResult<TopicPartition, ()>,
}

impl ElectLeadersResult {
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
