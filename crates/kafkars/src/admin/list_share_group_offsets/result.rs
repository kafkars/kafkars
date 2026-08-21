//! Public `ShareGroup` offset-listing result with Kafka throttle observation.

use std::time::Duration;

use crate::TopicPartition;

use super::{super::BatchResult, ShareGroupOffset};

/// Completed `ShareGroup` offset listing with deterministic per-partition outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupOffsetsResult {
    throttle_time: Duration,
    offsets: BatchResult<TopicPartition, ShareGroupOffset>,
}

impl ListShareGroupOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        offsets: BatchResult<TopicPartition, ShareGroupOffset>,
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

    /// Returns caller order for a selected query or topic/partition order for all.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ShareGroupOffset> {
        &self.offsets
    }

    /// Consumes this result into its deterministic per-partition outcomes.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ShareGroupOffset> {
        self.offsets
    }
}
