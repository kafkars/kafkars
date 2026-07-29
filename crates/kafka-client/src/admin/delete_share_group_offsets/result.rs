//! Public caller-ordered ShareGroup offset-deletion result.

use std::time::Duration;

use super::super::BatchResult;

/// Completed ShareGroup offset deletion with ordered per-topic outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsResult {
    throttle_time: Duration,
    topics: BatchResult<String, [u8; 16]>,
}

impl DeleteShareGroupOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        topics: BatchResult<String, [u8; 16]>,
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

    /// Returns per-topic outcomes in original caller order.
    pub const fn topics(&self) -> &BatchResult<String, [u8; 16]> {
        &self.topics
    }

    /// Consumes this result into per-topic outcomes in original caller order.
    pub fn into_topics(self) -> BatchResult<String, [u8; 16]> {
        self.topics
    }
}
