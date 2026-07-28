//! Inert reassignment-listing options with one explicit submission boundary.

use std::time::Duration;

use crate::bridge::{
    admin::AdminEngine, admin_list_partition_reassignments::ListPartitionReassignmentsAdminRequest,
};

use super::ListPartitionReassignments;

/// Inert query for explicitly selected or all active partition reassignments.
#[must_use = "call submit to admit the ListPartitionReassignments operation"]
pub struct ListPartitionReassignmentsBuilder {
    engine: AdminEngine,
    request: ListPartitionReassignmentsAdminRequest,
    timeout: Duration,
}

impl ListPartitionReassignmentsBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: ListPartitionReassignmentsAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Replaces the duration converted into an absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Attempts immediate bounded admission and returns one named observer.
    pub fn submit(self) -> ListPartitionReassignments {
        ListPartitionReassignments::from_bridge(
            self.engine
                .submit_list_partition_reassignments(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for ListPartitionReassignmentsBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListPartitionReassignmentsBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
