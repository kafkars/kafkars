//! Inert generic `IncrementalAlterConfigs` options with one submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_alter_configs_request::IncrementalAlterConfigsAdminRequest,
};

use super::IncrementalAlterConfigResources;

/// Inert ordered configuration-resource alteration request.
#[must_use = "call submit to admit the IncrementalAlterConfigs operation"]
pub struct IncrementalAlterConfigResourcesBuilder {
    engine: AdminEngine,
    request: IncrementalAlterConfigsAdminRequest,
    timeout: Duration,
}

impl IncrementalAlterConfigResourcesBuilder {
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

    /// Selects broker-side validation without mutating configurations.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.request = self.request.with_validate_only(validate_only);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures one public deadline and attempts immediate bounded admission.
    pub fn submit(self) -> IncrementalAlterConfigResources {
        IncrementalAlterConfigResources::from_bridge(
            self.engine
                .submit_incremental_alter_config_resources(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for IncrementalAlterConfigResourcesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncrementalAlterConfigResourcesBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
