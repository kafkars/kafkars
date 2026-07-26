//! Inert group-offset alteration intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_group_offsets::AlterConsumerGroupOffsetsAdminRequest,
};

use super::AlterConsumerGroupOffsets;

/// Inert caller-ordered committed-offset alteration for one consumer group.
#[must_use = "call submit to admit the AlterConsumerGroupOffsets operation"]
pub struct AlterConsumerGroupOffsetsBuilder {
    engine: AdminEngine,
    request: AlterConsumerGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl AlterConsumerGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterConsumerGroupOffsetsAdminRequest,
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
    ///
    /// This call is the public operation boundary. The engine captures its
    /// absolute deadline before canonicalization, validation, or admission.
    pub fn submit(self) -> AlterConsumerGroupOffsets {
        AlterConsumerGroupOffsets::from_bridge(
            self.engine
                .submit_alter_consumer_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterConsumerGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterConsumerGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
