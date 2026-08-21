//! Kafka metadata-quorum voter-removal entry point.

use super::Admin;
use crate::{
    admin::{RaftVoterIdentity, RemoveRaftVoterBuilder},
    bridge::remove_raft_voter::RemoveRaftVoterAdminRequest,
};

impl Admin {
    /// Builds inert intent to remove one exactly identified metadata-quorum voter.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`RemoveRaftVoterBuilder::submit`] is called.
    pub fn remove_raft_voter(&self, identity: RaftVoterIdentity) -> RemoveRaftVoterBuilder {
        RemoveRaftVoterBuilder::new(
            self.engine.clone(),
            RemoveRaftVoterAdminRequest::new(identity),
            self.engine.default_timeout(),
        )
    }
}
