//! Streams-group deletion result over the common group protocol result.

use std::time::Duration;

use crate::admin::{BatchResult, DeleteConsumerGroupsResult};

/// Completed streams-group deletion with caller-ordered per-group outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteStreamsGroupsResult {
    inner: DeleteConsumerGroupsResult,
}

impl DeleteStreamsGroupsResult {
    pub(crate) const fn from_consumer(inner: DeleteConsumerGroupsResult) -> Self {
        Self { inner }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.inner.throttle_time()
    }

    /// Returns per-group outcomes in original caller order.
    pub const fn groups(&self) -> &BatchResult<String, ()> {
        self.inner.groups()
    }

    /// Consumes this result into caller-ordered per-group outcomes.
    pub fn into_groups(self) -> BatchResult<String, ()> {
        self.inner.into_groups()
    }
}
