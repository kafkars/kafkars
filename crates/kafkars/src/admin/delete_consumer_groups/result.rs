//! Public caller-ordered Admin `DeleteConsumerGroups` result.

use std::time::Duration;

use super::super::BatchResult;

/// Completed Admin `DeleteConsumerGroups` operation with ordered per-group outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteConsumerGroupsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ()>,
}

impl DeleteConsumerGroupsResult {
    pub(crate) const fn new(throttle_time: Duration, groups: BatchResult<String, ()>) -> Self {
        Self {
            throttle_time,
            groups,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-group outcomes in original caller order.
    pub const fn groups(&self) -> &BatchResult<String, ()> {
        &self.groups
    }

    /// Consumes this result into caller-ordered per-group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ()> {
        self.groups
    }
}
