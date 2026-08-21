//! Ordered public resource-generic `DescribeConfigs` result.

use std::time::Duration;

use crate::admin::{BatchResult, ConfigEntry, ConfigResource};

/// Successful generic configuration batch in original request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigResourcesResult {
    throttle_time: Duration,
    resources: BatchResult<ConfigResource, Vec<ConfigEntry>>,
}

impl DescribeConfigResourcesResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        resources: BatchResult<ConfigResource, Vec<ConfigEntry>>,
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
    pub const fn resources(&self) -> &BatchResult<ConfigResource, Vec<ConfigEntry>> {
        &self.resources
    }

    /// Consumes the result into ordered resource outcomes.
    pub fn into_resources(self) -> BatchResult<ConfigResource, Vec<ConfigEntry>> {
        self.resources
    }
}
