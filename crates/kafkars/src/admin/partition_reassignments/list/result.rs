//! Public reassignment-listing result with Kafka throttle observation.

use std::time::Duration;

use crate::TopicPartition;

use super::PartitionReassignment;

/// Successful active reassignment listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPartitionReassignmentsResult {
    throttle_time: Duration,
    reassignments: Vec<(TopicPartition, PartitionReassignment)>,
}

impl ListPartitionReassignmentsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        reassignments: Vec<(TopicPartition, PartitionReassignment)>,
    ) -> Self {
        Self {
            throttle_time,
            reassignments,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns active rows in caller order for selected queries and strict
    /// topic-byte/partition order for all-active queries.
    pub fn reassignments(&self) -> &[(TopicPartition, PartitionReassignment)] {
        &self.reassignments
    }

    /// Consumes this result into deterministic active reassignment rows.
    pub fn into_reassignments(self) -> Vec<(TopicPartition, PartitionReassignment)> {
        self.reassignments
    }
}
