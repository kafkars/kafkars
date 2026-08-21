//! Inert `DeleteTopics` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::admin::{AdminEngine, DeleteAdminRequest};

use super::DeleteTopics;

/// Inert batched topic-deletion request.
#[must_use = "call submit to admit the DeleteTopics operation"]
pub struct DeleteTopicsBuilder {
    engine: AdminEngine,
    request: DeleteAdminRequest,
    timeout: Duration,
}

impl DeleteTopicsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DeleteAdminRequest,
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
    /// deadline before canonicalization, validation, or admission.
    pub fn submit(self) -> DeleteTopics {
        DeleteTopics::from_bridge(self.engine.submit_delete(self.request, self.timeout))
    }
}

impl std::fmt::Debug for DeleteTopicsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteTopicsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
