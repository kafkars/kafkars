//! Inert Admin `ListOffsets` intent with one explicit submission boundary.

use std::time::Duration;

use crate::ReadIsolation;
use crate::bridge::{admin::AdminEngine, admin_list_offsets::ListOffsetsAdminRequest};

use super::ListOffsets;

/// Inert caller-ordered Admin `ListOffsets` request.
#[must_use = "call submit to admit the ListOffsets operation"]
pub struct ListOffsetsBuilder {
    engine: AdminEngine,
    request: ListOffsetsAdminRequest,
    timeout: Duration,
}

impl ListOffsetsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListOffsetsAdminRequest,
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

    /// Selects whether unresolved transactional records influence listed offsets.
    ///
    /// The default is [`ReadIsolation::ReadUncommitted`].
    pub fn read_isolation(mut self, read_isolation: ReadIsolation) -> Self {
        self.request = self.request.with_read_isolation(read_isolation);
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    ///
    /// This is the public operation boundary. The engine captures its absolute
    /// deadline before validation or admission.
    pub fn submit(self) -> ListOffsets {
        ListOffsets::from_bridge(self.engine.submit_list_offsets(self.request, self.timeout))
    }
}

impl std::fmt::Debug for ListOffsetsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListOffsetsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
