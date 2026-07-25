//! Ordered public incremental configuration result with throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

/// Successful topic alteration batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResult {
    throttle_time: Duration,
    topics: BatchResult<String, ()>,
}

impl IncrementalAlterConfigsResult {
    pub(crate) const fn new(throttle_time: Duration, topics: BatchResult<String, ()>) -> Self {
        Self {
            throttle_time,
            topics,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns topic outcomes in original request order.
    pub const fn topics(&self) -> &BatchResult<String, ()> {
        &self.topics
    }

    /// Consumes the result into ordered topic outcomes.
    pub fn into_topics(self) -> BatchResult<String, ()> {
        self.topics
    }
}
