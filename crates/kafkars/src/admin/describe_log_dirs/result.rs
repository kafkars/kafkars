//! Caller-ordered per-broker log-directory results with throttle observation.

use std::time::Duration;

use super::{super::BatchResult, LogDirDescription};

/// Fully settled descriptions for the selected brokers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsResult {
    throttle_time: Duration,
    brokers: BatchResult<i32, BatchResult<String, LogDirDescription>>,
}

impl DescribeLogDirsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        brokers: BatchResult<i32, BatchResult<String, LogDirDescription>>,
    ) -> Self {
        Self {
            throttle_time,
            brokers,
        }
    }

    /// Returns the maximum nonnegative throttle observed across broker calls.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-broker outcomes in original caller order.
    ///
    /// Successful brokers contain per-path outcomes for every returned log
    /// directory, preserving both broker-level and log-directory-level errors.
    pub const fn brokers(&self) -> &BatchResult<i32, BatchResult<String, LogDirDescription>> {
        &self.brokers
    }

    /// Consumes this result into caller-ordered per-broker outcomes.
    pub fn into_brokers(self) -> BatchResult<i32, BatchResult<String, LogDirDescription>> {
        self.brokers
    }
}
