//! Fully settled unfiltered cluster-wide group listing result.

use std::time::Duration;

use super::{GroupListing, ListGroupsBrokerError};

/// Stable all-group listings with partial broker errors preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListGroupsResult {
    throttle_time: Duration,
    groups: Vec<GroupListing>,
    broker_errors: Vec<ListGroupsBrokerError>,
}

impl ListGroupsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        groups: Vec<GroupListing>,
        broker_errors: Vec<ListGroupsBrokerError>,
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

    /// Returns globally byte-sorted, group-ID-deduplicated listings.
    pub fn groups(&self) -> &[GroupListing] {
        &self.groups
    }

    /// Returns exact broker-scoped errors ordered by broker ID.
    pub fn broker_errors(&self) -> &[ListGroupsBrokerError] {
        &self.broker_errors
    }

    /// Consumes this result into stable listing and error parts.
    pub fn into_parts(self) -> (Duration, Vec<GroupListing>, Vec<ListGroupsBrokerError>) {
        (self.throttle_time, self.groups, self.broker_errors)
    }
}
