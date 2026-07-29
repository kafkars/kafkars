//! Stable successful configuration-resource listing.

use std::time::Duration;

use super::ConfigResource;

/// Bounded canonical configuration-resource identities returned by Kafka.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesResult {
    throttle: Duration,
    resources: Vec<ConfigResource>,
}

impl ListConfigResourcesResult {
    pub(crate) const fn new(throttle: Duration, resources: Vec<ConfigResource>) -> Self {
        Self {
            throttle,
            resources,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle(&self) -> Duration {
        self.throttle
    }

    /// Returns canonical resource identities in type-code then UTF-8 byte order.
    pub fn resources(&self) -> &[ConfigResource] {
        &self.resources
    }

    /// Consumes the result into its stable generated-free parts.
    pub fn into_parts(self) -> (Duration, Vec<ConfigResource>) {
        (self.throttle, self.resources)
    }
}
