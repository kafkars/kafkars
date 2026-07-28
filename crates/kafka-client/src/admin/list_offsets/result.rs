//! Public caller-ordered Admin `ListOffsets` result.

use std::time::Duration;

use crate::TopicPartition;

use super::{super::BatchResult, ListOffsetsResultInfo};

/// Successful deterministic Admin `ListOffsets` operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListOffsetsResult {
    throttle_time: Duration,
    offsets: BatchResult<TopicPartition, ListOffsetsResultInfo>,
}

impl ListOffsetsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        offsets: BatchResult<TopicPartition, ListOffsetsResultInfo>,
    ) -> Self {
        Self {
            throttle_time,
            offsets,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-partition outcomes in original caller order.
    pub const fn offsets(&self) -> &BatchResult<TopicPartition, ListOffsetsResultInfo> {
        &self.offsets
    }

    /// Consumes this result into caller-ordered per-partition outcomes.
    pub fn into_offsets(self) -> BatchResult<TopicPartition, ListOffsetsResultInfo> {
        self.offsets
    }
}
