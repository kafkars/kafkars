//! Public caller-ordered consumer-group description result.

use std::time::Duration;

use super::{super::BatchResult, ConsumerGroupDescription};

/// Successful deterministic `DescribeConsumerGroups` operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConsumerGroupsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ConsumerGroupDescription>,
}

impl DescribeConsumerGroupsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: BatchResult<String, ConsumerGroupDescription>,
    ) -> Self {
        Self {
            throttle_time,
            groups,
        }
    }

    /// Returns the maximum broker throttle observed across coordinator calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns exact per-group outcomes in caller order.
    pub const fn groups(&self) -> &BatchResult<String, ConsumerGroupDescription> {
        &self.groups
    }

    /// Consumes this result into caller-ordered per-group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ConsumerGroupDescription> {
        self.groups
    }
}
