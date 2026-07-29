//! Stable successful configuration-resource listing.

use super::ListConfigResource;

/// Bounded canonical configuration resources and Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResourcesListing {
    pub(super) throttle_time_ms: u32,
    pub(super) resources: Vec<ListConfigResource>,
}

impl ListConfigResourcesListing {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns resources in signed type-code then UTF-8 byte order.
    pub fn resources(&self) -> &[ListConfigResource] {
        &self.resources
    }

    /// Consumes the listing into stable generated-free parts.
    pub fn into_parts(self) -> (u32, Vec<ListConfigResource>) {
        (self.throttle_time_ms, self.resources)
    }
}
