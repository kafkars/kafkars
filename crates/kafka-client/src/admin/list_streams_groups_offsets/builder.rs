//! Inert multi-Streams-group offset intent over the shared `OffsetFetch` owner.

use std::time::Duration;

use crate::admin::ListConsumerGroupsOffsetsBuilder;

use super::ListStreamsGroupsOffsets;

/// Typed inert query for multiple Streams group identities.
#[must_use = "call submit to admit the ListStreamsGroupsOffsets operation"]
pub struct ListStreamsGroupsOffsetsBuilder {
    inner: ListConsumerGroupsOffsetsBuilder,
}

impl ListStreamsGroupsOffsetsBuilder {
    pub(crate) const fn from_consumer_groups(inner: ListConsumerGroupsOffsetsBuilder) -> Self {
        Self { inner }
    }

    /// Requires Kafka to reject offsets with pending transactional commits.
    pub fn require_stable(mut self, require_stable: bool) -> Self {
        self.inner = self.inner.require_stable(require_stable);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.deadline_after(timeout);
        self
    }

    /// Captures the public deadline and admits through the shared `OffsetFetch` owner.
    pub fn submit(self) -> ListStreamsGroupsOffsets {
        ListStreamsGroupsOffsets::from_consumer_groups(self.inner.submit())
    }
}

impl std::fmt::Debug for ListStreamsGroupsOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListStreamsGroupsOffsetsBuilder")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
