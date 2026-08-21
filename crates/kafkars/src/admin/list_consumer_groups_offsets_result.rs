//! Public multi-consumer-group offset result with aggregate throttle.

use std::time::Duration;

use super::{BatchResult, ListConsumerGroupOffsetsResult};

/// Caller-ordered consumer-group offset outcomes from one accepted operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsOffsetsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ListConsumerGroupOffsetsResult>,
}

impl ListConsumerGroupsOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: BatchResult<String, ListConsumerGroupOffsetsResult>,
    ) -> Self {
        Self {
            throttle_time,
            groups,
        }
    }

    /// Returns the maximum Kafka throttle observed across coordinator calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub const fn groups(&self) -> &BatchResult<String, ListConsumerGroupOffsetsResult> {
        &self.groups
    }

    /// Consumes this result into its caller-ordered group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ListConsumerGroupOffsetsResult> {
        self.groups
    }
}
