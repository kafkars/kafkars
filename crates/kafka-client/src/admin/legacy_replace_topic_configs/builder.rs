//! Inert options for destructive legacy topic configuration replacement.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, legacy_replace_topic_configs::LegacyReplaceTopicConfigsAdminRequest,
};

use super::LegacyReplaceTopicConfigs;

/// Inert ordered request to replace complete legacy topic configuration snapshots.
#[must_use = "call submit to admit the destructive legacy replacement operation"]
pub struct LegacyReplaceTopicConfigsBuilder {
    engine: AdminEngine,
    request: LegacyReplaceTopicConfigsAdminRequest,
    timeout: Duration,
}

impl LegacyReplaceTopicConfigsBuilder {
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

    /// Selects broker-side validation without replacing topic configurations.
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
    pub fn submit(self) -> LegacyReplaceTopicConfigs {
        LegacyReplaceTopicConfigs::from_bridge(
            self.engine
                .submit_legacy_replace_topic_configs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for LegacyReplaceTopicConfigsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyReplaceTopicConfigsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
