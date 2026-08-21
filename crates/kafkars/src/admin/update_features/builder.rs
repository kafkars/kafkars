//! Inert finalized-feature update intent with one submission boundary.

use std::time::Duration;

use crate::bridge::admin::AdminEngine;

use super::{UpdateFeatures, UpdateFeaturesRequest};

/// Inert caller-ordered finalized-feature update request.
#[must_use = "call submit to admit the UpdateFeatures operation"]
pub struct UpdateFeaturesBuilder {
    engine: AdminEngine,
    request: UpdateFeaturesRequest,
    timeout: Duration,
}

impl UpdateFeaturesBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: UpdateFeaturesRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects broker-side validation without changing finalized features.
    ///
    /// The default is `false`.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.request = self.request.with_validate_only(validate_only);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures one public deadline, validates the request, and attempts admission.
    pub fn submit(self) -> UpdateFeatures {
        UpdateFeatures::from_bridge(
            self.engine
                .submit_update_features(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for UpdateFeaturesBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UpdateFeaturesBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
