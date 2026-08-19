//! Inert `ShareGroup` offset-deletion intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, delete_share_group_offsets::DeleteShareGroupOffsetsAdminRequest,
};

use super::DeleteShareGroupOffsets;

/// Inert caller-ordered topic offset deletion for one `ShareGroup`.
#[must_use = "call submit to admit the DeleteShareGroupOffsets operation"]
pub struct DeleteShareGroupOffsetsBuilder {
    engine: AdminEngine,
    request: DeleteShareGroupOffsetsAdminRequest,
    timeout: Duration,
}

impl DeleteShareGroupOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DeleteShareGroupOffsetsAdminRequest,
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
    pub fn submit(self) -> DeleteShareGroupOffsets {
        DeleteShareGroupOffsets::from_bridge(
            self.engine
                .submit_delete_share_group_offsets(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DeleteShareGroupOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteShareGroupOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
