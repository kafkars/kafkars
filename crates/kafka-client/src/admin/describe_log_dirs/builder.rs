//! Inert caller-ordered broker log-directory description intent.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, admin_describe_log_dirs::DescribeLogDirsAdminRequest};

use super::DescribeLogDirs;

/// Inert request for all log directories on selected brokers.
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
