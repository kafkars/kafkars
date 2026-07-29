//! Inert topic-ID DeleteTopics options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::admin::{AdminEngine, DeleteAdminRequest};

use super::DeleteTopicsById;

/// Inert caller-ordered topic-ID deletion request.
#[must_use = "call submit to admit the DeleteTopicsById operation"]
pub struct DeleteTopicsByIdBuilder {
    engine: AdminEngine,
    request: DeleteAdminRequest,
    timeout: Duration,
}

impl DeleteTopicsByIdBuilder {
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

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> DeleteTopicsById {
        DeleteTopicsById::from_bridge(self.engine.submit_delete_by_id(self.request, self.timeout))
    }
}

impl std::fmt::Debug for DeleteTopicsByIdBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteTopicsByIdBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
