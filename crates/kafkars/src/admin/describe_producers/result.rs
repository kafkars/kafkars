//! Public caller-ordered Admin `DescribeProducers` result.

use std::time::Duration;

use crate::TopicPartition;

use super::{super::BatchResult, ProducerState};

/// Completed producer descriptions with maximum throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeProducersResult {
    throttle_time: Duration,
    partitions: BatchResult<TopicPartition, Vec<ProducerState>>,
}

impl DescribeProducersResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        partitions: BatchResult<TopicPartition, Vec<ProducerState>>,
    ) -> Self {
        Self {
            throttle_time,
            partitions,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-partition outcomes in original caller order.
    ///
    /// Each successful partition's producer states are ordered by producer ID.
    pub const fn partitions(&self) -> &BatchResult<TopicPartition, Vec<ProducerState>> {
        &self.partitions
    }

    /// Consumes this result into caller-ordered per-partition outcomes.
    pub fn into_partitions(self) -> BatchResult<TopicPartition, Vec<ProducerState>> {
        self.partitions
    }
}
