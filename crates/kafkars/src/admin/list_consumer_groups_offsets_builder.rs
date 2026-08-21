//! Inert multi-consumer-group offset options with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_group_offsets::ListConsumerGroupsOffsetsAdminRequest,
};

use super::ListConsumerGroupsOffsets;

/// Inert caller-ordered all-partition queries for multiple consumer groups.
#[must_use = "call submit to admit the ListConsumerGroupsOffsets operation"]
pub struct ListConsumerGroupsOffsetsBuilder {
    engine: AdminEngine,
    request: ListConsumerGroupsOffsetsAdminRequest,
    timeout: Duration,
}

impl ListConsumerGroupsOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListConsumerGroupsOffsetsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Requires Kafka to reject offsets with pending transactional commits.
    pub fn require_stable(mut self, require_stable: bool) -> Self {
        self.request = self.request.with_require_stable(require_stable);
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> ListConsumerGroupsOffsets {
        ListConsumerGroupsOffsets::from_bridge(
            self.engine
                .submit_list_consumer_groups_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListConsumerGroupsOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupsOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
