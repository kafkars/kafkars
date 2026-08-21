//! Typed Streams-group result over deterministic consumer-group offset facts.

use std::time::Duration;

use crate::{
    TopicPartition,
    admin::{BatchResult, ConsumerGroupOffset, ListConsumerGroupOffsetsResult},
};

/// Successful deterministic Streams-group offset listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListStreamsGroupOffsetsResult {
    inner: ListConsumerGroupOffsetsResult,
}

impl ListStreamsGroupOffsetsResult {
    pub(crate) const fn from_consumer_group(inner: ListConsumerGroupOffsetsResult) -> Self {
        Self { inner }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.inner.throttle_time()
    }

    /// Returns topic-partition outcomes in deterministic topic/partition order.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ConsumerGroupOffset> {
        self.inner.offsets()
    }

    /// Consumes this result into deterministic topic-partition outcomes.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ConsumerGroupOffset> {
        self.inner.into_offsets()
    }
}
