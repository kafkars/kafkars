//! Public group-offset listing result with Kafka throttle observation.

use std::time::Duration;

use crate::TopicPartition;

use super::{BatchResult, ConsumerGroupOffset};

/// Successful deterministic consumer-group offset listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupOffsetsResult {
    throttle_time: Duration,
    offsets: BatchResult<TopicPartition, ConsumerGroupOffset>,
}

impl ListConsumerGroupOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        offsets: BatchResult<TopicPartition, ConsumerGroupOffset>,
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

    /// Returns topic-partition outcomes in deterministic topic/partition order.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ConsumerGroupOffset> {
        &self.offsets
    }

    /// Consumes this result into deterministic topic-partition outcomes.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ConsumerGroupOffset> {
        self.offsets
    }
}
