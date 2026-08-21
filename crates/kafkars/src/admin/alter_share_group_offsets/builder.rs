//! Inert `ShareGroup` offset-alteration intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, alter_share_group_offsets::AlterShareGroupOffsetsAdminRequest,
};

use super::AlterShareGroupOffsets;

/// Inert caller-ordered partition-offset alteration for one `ShareGroup`.
#[must_use = "call submit to admit the AlterShareGroupOffsets operation"]
pub struct AlterShareGroupOffsetsBuilder {
    engine: AdminEngine,
    request: AlterShareGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl AlterShareGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterShareGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> AlterShareGroupOffsets {
        AlterShareGroupOffsets::from_bridge(
            self.engine
                .submit_alter_share_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterShareGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterShareGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
