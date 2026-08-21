//! Ordered public resource-generic incremental configuration result.

use std::time::Duration;

use crate::admin::{BatchResult, ConfigResource};

/// Successful generic alteration batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigResourcesResult {
    throttle_time: Duration,
    resources: BatchResult<ConfigResource, ()>,
}

impl IncrementalAlterConfigResourcesResult {
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
