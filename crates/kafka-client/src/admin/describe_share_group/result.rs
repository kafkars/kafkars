//! Public result for one ShareGroup description.

use std::time::Duration;

use super::ShareGroupDescription;

/// Completed ShareGroup description plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupResult {
    throttle_time: Duration,
    description: ShareGroupDescription,
}

impl DescribeShareGroupResult {
    pub(crate) const fn new(throttle_time: Duration, description: ShareGroupDescription) -> Self {
        Self {
            throttle_time,
            description,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the typed description.
    pub const fn description(&self) -> &ShareGroupDescription {
        &self.description
    }

    /// Consumes this result into the typed description.
    pub fn into_description(self) -> ShareGroupDescription {
        self.description
    }
}
