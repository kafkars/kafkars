//! Inert reassignment intent with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_partition_reassignments::AlterPartitionReassignmentsAdminRequest,
};

use super::AlterPartitionReassignments;

/// Inert caller-ordered partition-reassignment alteration.
#[must_use = "call submit to admit the AlterPartitionReassignments operation"]
pub struct AlterPartitionReassignmentsBuilder {
    engine: AdminEngine,
    request: AlterPartitionReassignmentsAdminRequest,
    timeout: Duration,
}

impl AlterPartitionReassignmentsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AlterPartitionReassignmentsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces whether Kafka may change a partition's replication factor.
    ///
    /// The default is `true`. Selecting `false` requires a broker that
    /// supports API-key 45 version 1.
    pub fn allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.request = self.request.with_allow_replication_factor_change(allow);
        self
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts bounded admission and returns one named observer.
    pub fn submit(self) -> AlterPartitionReassignments {
        AlterPartitionReassignments::from_bridge(
            self.engine
                .submit_alter_partition_reassignments(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AlterPartitionReassignmentsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterPartitionReassignmentsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
