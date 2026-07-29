//! Inert Streams-group offset intent delegated to the shared OffsetFetch path.

use std::time::Duration;

use crate::admin::ListConsumerGroupOffsetsBuilder;

use super::ListStreamsGroupOffsets;

/// Typed inert query for every broker-visible committed offset in one Streams group.
#[must_use = "call submit to admit the ListStreamsGroupOffsets operation"]
pub struct ListStreamsGroupOffsetsBuilder {
    inner: ListConsumerGroupOffsetsBuilder,
}

impl ListStreamsGroupOffsetsBuilder {
    pub(crate) const fn from_consumer_group(inner: ListConsumerGroupOffsetsBuilder) -> Self {
        Self { inner }
    }

    /// Requires Kafka to reject offsets with pending transactional commits.
    pub fn require_stable(mut self, require_stable: bool) -> Self {
        self.inner = self.inner.require_stable(require_stable);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub fn deadline_after(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.deadline_after(timeout);
        self
    }

    /// Attempts immediate bounded admission through the existing OffsetFetch owner.
    pub fn submit(self) -> ListStreamsGroupOffsets {
        ListStreamsGroupOffsets::from_consumer_group(self.inner.submit())
    }
}

impl std::fmt::Debug for ListStreamsGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListStreamsGroupOffsetsBuilder")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
