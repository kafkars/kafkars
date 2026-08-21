//! Public group-offset alteration result with Kafka throttle observation.

use std::time::Duration;

use crate::{TopicPartition, admin::BatchResult};

/// Successful deterministic consumer-group offset alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetsResult {
    throttle_time: Duration,
    offsets: BatchResult<TopicPartition, ()>,
}

impl AlterConsumerGroupOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        offsets: BatchResult<TopicPartition, ()>,
    ) -> Self {
        Self {
            throttle_time,
            offsets,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-partition outcomes in original caller order.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ()> {
        &self.offsets
    }

    /// Consumes this result into per-partition outcomes in original caller order.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ()> {
        self.offsets
    }
}
