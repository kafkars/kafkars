//! Inert Admin `DeleteRecords` intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_delete_records::DeleteRecordsAdminRequest};

use super::DeleteRecords;

/// Inert caller-ordered Admin `DeleteRecords` request.
#[must_use = "call submit to admit the DeleteRecords operation"]
pub struct DeleteRecordsBuilder {
    engine: AdminEngine,
    request: DeleteRecordsAdminRequest,
    timeout: Duration,
}

impl DeleteRecordsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DeleteRecordsAdminRequest,
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
    /// This is the public operation boundary. The engine captures its absolute
    /// deadline before validation or admission.
    pub fn submit(self) -> DeleteRecords {
        DeleteRecords::from_bridge(
            self.engine
                .submit_delete_records(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DeleteRecordsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteRecordsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
