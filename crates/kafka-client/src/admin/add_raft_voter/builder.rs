//! Inert metadata-quorum voter-addition intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{add_raft_voter::AddRaftVoterAdminRequest, admin::AdminEngine};

use super::AddRaftVoter;

/// Inert request to add one identified voter and its advertised endpoints.
#[must_use = "call submit to admit the AddRaftVoter operation"]
pub struct AddRaftVoterBuilder {
    engine: AdminEngine,
    request: AddRaftVoterAdminRequest,
    timeout: Duration,
}

impl AddRaftVoterBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: AddRaftVoterAdminRequest,
        timeout: Duration,
    ) -> Self {
        Self {
            engine,
            request,
            timeout,
        }
    }

    /// Supplies the cluster identity Kafka must match before changing the quorum.
    pub fn cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.request = self.request.with_cluster_id(cluster_id.into());
        self
    }

    /// Replaces the duration converted into one absolute deadline at submission.
    pub const fn deadline_after(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Captures the public deadline before conversion and attempts bounded admission.
    pub fn submit(self) -> AddRaftVoter {
        AddRaftVoter::from_bridge(
            self.engine
                .submit_add_raft_voter(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for AddRaftVoterBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AddRaftVoterBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
