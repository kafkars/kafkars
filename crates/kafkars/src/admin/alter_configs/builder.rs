//! Inert `IncrementalAlterConfigs` options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_alter_configs_request::IncrementalAlterConfigsAdminRequest,
};

use super::IncrementalAlterConfigs;

/// Inert ordered topic configuration alteration request.
#[must_use = "call submit to admit the IncrementalAlterConfigs operation"]
pub struct IncrementalAlterConfigsBuilder {
    engine: AdminEngine,
    request: IncrementalAlterConfigsAdminRequest,
    timeout: Duration,
}

impl IncrementalAlterConfigsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: IncrementalAlterConfigsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without mutating topic configurations.
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
    pub fn submit(self) -> IncrementalAlterConfigs {
        IncrementalAlterConfigs::from_bridge(
            self.engine
                .submit_incremental_alter_configs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for IncrementalAlterConfigsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncrementalAlterConfigsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
