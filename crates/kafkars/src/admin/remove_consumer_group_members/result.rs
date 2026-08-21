//! Public static-member removal result with Kafka throttle observation.

use std::time::Duration;

use crate::admin::BatchResult;

/// Successful deterministic consumer-group member removal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveConsumerGroupMembersResult {
    throttle_time: Duration,
    members: BatchResult<String, ()>,
}

impl RemoveConsumerGroupMembersResult {
    pub(crate) const fn new(throttle_time: Duration, members: BatchResult<String, ()>) -> Self {
        Self {
            throttle_time,
            members,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-member outcomes in original caller order.
    pub const fn members(&self) -> &BatchResult<String, ()> {
        &self.members
    }

    /// Consumes this result into caller-ordered per-member outcomes.
    pub fn into_members(self) -> BatchResult<String, ()> {
        self.members
    }
}
