//! Public caller-ordered Admin `DeleteRecords` result.

use std::time::Duration;

use crate::TopicPartition;

use super::{super::BatchResult, DeleteRecordsResultInfo};

/// Completed Admin `DeleteRecords` operation with ordered per-target outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsResult {
    throttle_time: Duration,
    records: BatchResult<TopicPartition, DeleteRecordsResultInfo>,
}

impl DeleteRecordsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        records: BatchResult<TopicPartition, DeleteRecordsResultInfo>,
    ) -> Self {
        Self {
            throttle_time,
            records,
        }
    }

    /// Returns the maximum nonnegative broker throttle observed.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-partition outcomes in original caller order.
    pub const fn records(&self) -> &BatchResult<TopicPartition, DeleteRecordsResultInfo> {
        &self.records
    }

    /// Consumes this result into caller-ordered per-partition outcomes.
    pub fn into_records(self) -> BatchResult<TopicPartition, DeleteRecordsResultInfo> {
        self.records
    }
}
