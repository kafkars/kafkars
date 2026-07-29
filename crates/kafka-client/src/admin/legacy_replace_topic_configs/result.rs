//! Ordered public result of legacy topic configuration replacement.

use std::time::Duration;

use crate::admin::BatchResult;

/// Successful legacy replacement batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyReplaceTopicConfigsResult {
    throttle_time: Duration,
    topics: BatchResult<String, ()>,
}

impl LegacyReplaceTopicConfigsResult {
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
