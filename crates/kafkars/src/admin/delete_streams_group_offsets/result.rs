//! Typed Streams-group view of the caller-ordered offset-deletion result.

use std::time::Duration;

use crate::{BatchResult, DeleteConsumerGroupOffsetsResult, TopicPartition};

/// Completed Streams-group offset deletion with original caller ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteStreamsGroupOffsetsResult {
    inner: DeleteConsumerGroupOffsetsResult,
}

impl DeleteStreamsGroupOffsetsResult {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroupOffsetsResult) -> Self {
        Self { inner }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.inner.throttle_time()
    }

    /// Returns per-partition outcomes in original caller order.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ()> {
        self.inner.offsets()
    }

    /// Consumes this result into per-partition outcomes in original caller order.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ()> {
        self.inner.into_offsets()
    }
}
