//! Successful cluster-wide consumer-group listing result.

use std::time::Duration;

use super::{ConsumerGroupListing, ListConsumerGroupsBrokerError};

/// Fully settled cluster-wide listing with partial broker errors preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConsumerGroupsResult {
    throttle_time: Duration,
    groups: Vec<ConsumerGroupListing>,
    broker_errors: Vec<ListConsumerGroupsBrokerError>,
}

impl ListConsumerGroupsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: Vec<ConsumerGroupListing>,
        broker_errors: Vec<ListConsumerGroupsBrokerError>,
    ) -> Self {
        Self {
            throttle_time,
            groups,
            broker_errors,
        }
    }

    /// Returns the maximum throttle observed across exact-broker calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns globally byte-sorted, group-ID-deduplicated consumer groups.
    pub fn groups(&self) -> &[ConsumerGroupListing] {
        &self.groups
    }

    /// Returns exact broker-scoped errors ordered by broker ID.
    pub fn broker_errors(&self) -> &[ListConsumerGroupsBrokerError] {
        &self.broker_errors
    }

    /// Consumes this result into stable listing and error parts.
    pub fn into_parts(
        self,
    ) -> (
        Duration,
        Vec<ConsumerGroupListing>,
        Vec<ListConsumerGroupsBrokerError>,
    ) {
        (self.throttle_time, self.groups, self.broker_errors)
    }
}
