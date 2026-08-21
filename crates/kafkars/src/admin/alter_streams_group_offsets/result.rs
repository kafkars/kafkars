//! Typed `StreamsGroup` result over the shared consumer-group terminal value.

use std::time::Duration;

use crate::{AlterConsumerGroupOffsetsResult, TopicPartition, admin::BatchResult};

/// Successful `StreamsGroup` offset alteration in original caller order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterStreamsGroupOffsetsResult {
    inner: AlterConsumerGroupOffsetsResult,
}

impl AlterStreamsGroupOffsetsResult {
    pub(crate) const fn from_consumer_group(inner: AlterConsumerGroupOffsetsResult) -> Self {
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
