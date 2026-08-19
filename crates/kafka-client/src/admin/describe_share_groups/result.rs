//! Caller-ordered result of describing multiple `ShareGroups`.

use std::time::Duration;

use crate::{BatchResult, admin::ShareGroupDescription};

/// The completed response for a caller-ordered `ShareGroup` description batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ShareGroupDescription>,
}

impl DescribeShareGroupsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: BatchResult<String, ShareGroupDescription>,
    ) -> Self {
        Self {
            throttle_time,
            groups,
        }
    }

    /// Returns the maximum broker throttle observed across the batch.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns group results in the caller's requested order.
    pub const fn groups(&self) -> &BatchResult<String, ShareGroupDescription> {
        &self.groups
    }

    /// Consumes the response and returns group results in caller order.
    pub fn into_groups(self) -> BatchResult<String, ShareGroupDescription> {
        self.groups
    }
}
