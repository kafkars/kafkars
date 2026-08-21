//! Inert caller-ordered selected-replica log-directory description intent.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_describe_replica_log_dirs::DescribeReplicaLogDirsAdminRequest,
};

use super::DescribeReplicaLogDirs;

/// Inert request for log-directory placements of selected replicas.
#[must_use = "call submit to admit the DescribeReplicaLogDirs operation"]
pub struct DescribeReplicaLogDirsBuilder {
    engine: AdminEngine,
    request: DescribeReplicaLogDirsAdminRequest,
    timeout: Duration,
}

impl DescribeReplicaLogDirsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: DescribeReplicaLogDirsAdminRequest,
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
    pub fn submit(self) -> DescribeReplicaLogDirs {
        DescribeReplicaLogDirs::from_bridge(
            self.engine
                .submit_describe_replica_log_dirs(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for DescribeReplicaLogDirsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeReplicaLogDirsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
