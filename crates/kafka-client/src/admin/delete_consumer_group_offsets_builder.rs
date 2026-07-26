//! Inert group-offset deletion intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_group_offset_delete_request::DeleteConsumerGroupOffsetsAdminRequest,
};

use super::DeleteConsumerGroupOffsets;

/// Inert caller-ordered committed-offset deletion for one consumer group.
#[must_use = "call submit to admit the DeleteConsumerGroupOffsets operation"]
pub struct DeleteConsumerGroupOffsetsBuilder {
    engine: AdminEngine,
    request: DeleteConsumerGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl DeleteConsumerGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DeleteConsumerGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> DeleteConsumerGroupOffsets {
        DeleteConsumerGroupOffsets::from_bridge(
            self.engine
                .submit_delete_consumer_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DeleteConsumerGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteConsumerGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
