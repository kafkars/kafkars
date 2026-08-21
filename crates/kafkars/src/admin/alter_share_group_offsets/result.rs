//! Public caller-ordered `ShareGroup` offset-alteration result.

use std::time::Duration;

use crate::TopicPartition;

use super::super::BatchResult;

/// Completed `ShareGroup` offset alteration with ordered per-partition outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsResult {
    throttle_time: Duration,
    offsets: BatchResult<TopicPartition, [u8; 16]>,
}

impl AlterShareGroupOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        offsets: BatchResult<TopicPartition, [u8; 16]>,
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
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, [u8; 16]> {
        &self.offsets
    }

    /// Consumes this result into per-partition outcomes in original caller order.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, [u8; 16]> {
        self.offsets
    }
}
