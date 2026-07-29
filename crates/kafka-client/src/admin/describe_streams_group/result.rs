//! Public result for one StreamsGroup description.

use std::time::Duration;

use super::StreamsGroupDescription;

/// Completed StreamsGroup description plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupResult {
    throttle_time: Duration,
    description: StreamsGroupDescription,
}

impl DescribeStreamsGroupResult {
    pub(crate) const fn new(throttle_time: Duration, description: StreamsGroupDescription) -> Self {
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
    pub const fn description(&self) -> &StreamsGroupDescription {
        &self.description
    }

    /// Consumes this result into the typed description.
    pub fn into_description(self) -> StreamsGroupDescription {
        self.description
    }
}
