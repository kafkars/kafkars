//! Stable successful client-metrics configuration-resource listing.

use std::time::Duration;

/// Bounded client-metrics resource names returned by one Kafka broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListClientMetricsResourcesResult {
    throttle: Duration,
    resources: Vec<String>,
}

impl ListClientMetricsResourcesResult {
    pub(crate) const fn new(throttle: Duration, resources: Vec<String>) -> Self {
        Self {
            throttle,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle(&self) -> Duration {
        self.throttle
    }

    /// Returns unique resource names in strict UTF-8 byte order.
    pub fn resources(&self) -> &[String] {
        &self.resources
    }

    /// Consumes the result into its stable generated-free parts.
    pub fn into_parts(self) -> (Duration, Vec<String>) {
        (self.throttle, self.resources)
    }
}
