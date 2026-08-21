//! Inert `CreatePartitions` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::admin::{AdminEngine, PartitionsAdminRequest};

use super::CreatePartitions;

/// Inert automatic or explicit-placement partition-increase request.
#[must_use = "call submit to admit the CreatePartitions operation"]
pub struct CreatePartitionsBuilder {
    engine: AdminEngine,
    request: PartitionsAdminRequest,
    timeout: Duration,
}

impl CreatePartitionsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: PartitionsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without mutating partition counts.
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
    /// The engine captures the absolute deadline before validation or admission.
    pub fn submit(self) -> CreatePartitions {
        CreatePartitions::from_bridge(
            self.engine
                .submit_create_partitions(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for CreatePartitionsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreatePartitionsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
