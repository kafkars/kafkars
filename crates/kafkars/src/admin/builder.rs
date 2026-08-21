//! Inert `CreateTopics` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::admin::{AdminEngine, AdminRequest};

use super::CreateTopics;

/// Inert batched topic-creation request.
#[must_use = "call submit to admit the CreateTopics operation"]
pub struct CreateTopicsBuilder {
    engine: AdminEngine,
    request: AdminRequest,
    timeout: Duration,
}

impl CreateTopicsBuilder {
    pub(crate) const fn new(engine: AdminEngine, request: AdminRequest, timeout: Duration) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without mutating cluster state.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.request = self.request.with_validate_only(validate_only);
        self
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
    pub fn submit(self) -> CreateTopics {
        CreateTopics::from_bridge(self.engine.submit(self.request, self.timeout))
    }
}

impl std::fmt::Debug for CreateTopicsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateTopicsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
