//! Public multi-ShareGroup result with aggregate throttle.

use std::time::Duration;

use super::super::{BatchResult, ListShareGroupOffsetsResult};

/// Caller-ordered ShareGroup offset outcomes from one accepted operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListShareGroupsOffsetsResult {
    throttle_time: Duration,
    groups: BatchResult<String, ListShareGroupOffsetsResult>,
}

impl ListShareGroupsOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: BatchResult<String, ListShareGroupOffsetsResult>,
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

    /// Returns exactly one outcome per requested ShareGroup in caller order.
    pub const fn groups(&self) -> &BatchResult<String, ListShareGroupOffsetsResult> {
        &self.groups
    }

    /// Consumes this result into caller-ordered ShareGroup outcomes.
    pub fn into_groups(self) -> BatchResult<String, ListShareGroupOffsetsResult> {
        self.groups
    }
}
