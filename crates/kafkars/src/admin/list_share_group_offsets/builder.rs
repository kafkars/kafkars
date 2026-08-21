//! Inert `ShareGroup` offset-listing intent with one submission boundary.

use std::time::Duration;

use crate::{
    TopicPartition,
    bridge::{admin::AdminEngine, list_share_group_offsets::ListShareGroupOffsetsAdminRequest},
};

use super::ListShareGroupOffsets;

/// Inert all-partition or caller-selected offset query for one `ShareGroup`.
#[must_use = "call submit to admit the ListShareGroupOffsets operation"]
pub struct ListShareGroupOffsetsBuilder {
    engine: AdminEngine,
    request: ListShareGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl ListShareGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListShareGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects a nonempty caller-ordered set of topic-partitions.
    ///
    /// Validation remains deferred until [`Self::submit`] captures the public
    /// absolute deadline. Passing an empty iterator is therefore rejected as
    /// a definitely-unsent configuration error at submission.
    pub fn partitions<I>(mut self, partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        self.request = self
            .request
            .with_partitions(partitions.into_iter().collect());
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListShareGroupOffsets {
        ListShareGroupOffsets::from_bridge(
            self.engine
                .submit_list_share_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListShareGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListShareGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
