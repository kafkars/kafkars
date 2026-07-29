//! Inert caller-ordered broker log-directory description intent.

use std::time::Duration;

use crate::{
    TopicPartition,
    bridge::{admin::AdminEngine, admin_describe_log_dirs::DescribeLogDirsAdminRequest},
};

use super::DescribeLogDirs;

/// Inert request for log directories on selected brokers.
#[must_use = "call submit to admit the DescribeLogDirs operation"]
pub struct DescribeLogDirsBuilder {
    engine: AdminEngine,
    request: DescribeLogDirsAdminRequest,
    timeout: Duration,
}

impl DescribeLogDirsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeLogDirsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Selects a nonempty caller-ordered set of topic-partitions.
    ///
    /// This replaces any earlier selection. Validation remains deferred until
    /// [`Self::submit`] captures the public absolute deadline. Passing an empty
    /// iterator is therefore rejected as a definitely-unsent configuration
    /// error at submission.
    pub fn partitions<I>(mut self, partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        self.request = self
            .request
            .with_partitions(partitions.into_iter().collect());
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline and attempts immediate bounded admission.
    ///
    /// The bridge validates the nonempty, caller-ordered set of unique
    /// nonnegative broker IDs after capturing that deadline.
    pub fn submit(self) -> DescribeLogDirs {
        DescribeLogDirs::from_bridge(
            self.engine
                .submit_describe_log_dirs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeLogDirsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeLogDirsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
