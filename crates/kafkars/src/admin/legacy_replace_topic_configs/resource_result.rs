//! Ordered public result of generic legacy configuration replacement.

use std::time::Duration;

use crate::admin::{BatchResult, ConfigResource};

/// Successful generic replacement batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyReplaceConfigResourcesResult {
    throttle_time: Duration,
    resources: BatchResult<ConfigResource, ()>,
}

impl LegacyReplaceConfigResourcesResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        resources: BatchResult<ConfigResource, ()>,
    ) -> Self {
        Self {
            throttle_time,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns resource outcomes in original request order.
    pub const fn resources(&self) -> &BatchResult<ConfigResource, ()> {
        &self.resources
    }

    /// Consumes the result into ordered resource outcomes.
    pub fn into_resources(self) -> BatchResult<ConfigResource, ()> {
        self.resources
    }
}
