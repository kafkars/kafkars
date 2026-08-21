//! Inert options for destructive generic legacy configuration replacement.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, legacy_replace_topic_configs::LegacyReplaceTopicConfigsAdminRequest,
};

use super::LegacyReplaceConfigResources;

/// Inert ordered request to replace complete configuration-resource snapshots.
#[must_use = "call submit to admit the destructive legacy replacement operation"]
pub struct LegacyReplaceConfigResourcesBuilder {
    engine: AdminEngine,
    request: LegacyReplaceTopicConfigsAdminRequest,
    timeout: Duration,
}

impl LegacyReplaceConfigResourcesBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: LegacyReplaceTopicConfigsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without replacing configurations.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.request = self.request.with_validate_only(validate_only);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures one deadline and attempts bounded destructive admission.
    pub fn submit(self) -> LegacyReplaceConfigResources {
        LegacyReplaceConfigResources::from_bridge(
            self.engine
                .submit_legacy_replace_config_resources(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for LegacyReplaceConfigResourcesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyReplaceConfigResourcesBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
