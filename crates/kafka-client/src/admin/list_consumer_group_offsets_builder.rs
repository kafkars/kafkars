//! Inert consumer-group offset options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_group_offsets_request::ListConsumerGroupOffsetsAdminRequest,
};

use super::ListConsumerGroupOffsets;

/// Inert query for every broker-visible committed offset in one group.
#[must_use = "call submit to admit the ListConsumerGroupOffsets operation"]
pub struct ListConsumerGroupOffsetsBuilder {
    engine: AdminEngine,
    request: ListConsumerGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl ListConsumerGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListConsumerGroupOffsetsAdminRequest,
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

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> ListConsumerGroupOffsets {
        ListConsumerGroupOffsets::from_bridge(
            self.engine
                .submit_list_consumer_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListConsumerGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListConsumerGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
