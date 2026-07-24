//! Ordered public `DescribeConfigs` result with broker throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

use super::ConfigEntry;

/// Successful topic-configuration batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    throttle_time: Duration,
    topics: BatchResult<String, Vec<ConfigEntry>>,
}

impl DescribeConfigsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        topics: BatchResult<String, Vec<ConfigEntry>>,
    ) -> Self {
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
    pub const fn topics(&self) -> &BatchResult<String, Vec<ConfigEntry>> {
        &self.topics
    }

    /// Consumes the result into ordered topic outcomes.
    pub fn into_topics(self) -> BatchResult<String, Vec<ConfigEntry>> {
        self.topics
    }
}
