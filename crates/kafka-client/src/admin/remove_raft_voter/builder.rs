//! Inert metadata-quorum voter-removal intent with one submission boundary.

use std::time::Duration;

use crate::bridge::{admin::AdminEngine, remove_raft_voter::RemoveRaftVoterAdminRequest};

use super::RemoveRaftVoter;

/// Inert request to remove one exactly identified metadata-quorum voter.
#[must_use = "call submit to admit the RemoveRaftVoter operation"]
pub struct RemoveRaftVoterBuilder {
    engine: AdminEngine,
    request: RemoveRaftVoterAdminRequest,
    timeout: Duration,
}

impl RemoveRaftVoterBuilder {
    pub(crate) const fn new(
        engine: AdminEngine,
        request: RemoveRaftVoterAdminRequest,
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
    pub fn submit(self) -> RemoveRaftVoter {
        RemoveRaftVoter::from_bridge(
            self.engine
                .submit_remove_raft_voter(self.request, self.timeout),
        )
    }
}

impl std::fmt::Debug for RemoveRaftVoterBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoveRaftVoterBuilder")
            .field("request", &self.request)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}
