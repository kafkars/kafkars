//! Kafka metadata-quorum voter-addition entry point.

use super::Admin;
use crate::{
    admin::{AddRaftVoterBuilder, RaftVoterEndpoint, RaftVoterIdentity},
    bridge::add_raft_voter::AddRaftVoterAdminRequest,
};

impl Admin {
    /// Builds inert intent to add one exactly identified metadata-quorum voter.
    ///
    /// No timeout starts and no operation is admitted until
    /// [`AddRaftVoterBuilder::submit`] is called.
    pub fn add_raft_voter<I>(
        &self,
        identity: RaftVoterIdentity,
        endpoints: I,
    ) -> AddRaftVoterBuilder
    where
        I: IntoIterator<Item = RaftVoterEndpoint>,
    {
        AddRaftVoterBuilder::new(
            self.engine.clone(),
            AddRaftVoterAdminRequest::new(identity, endpoints.into_iter().collect()),
            self.engine.default_timeout(),
        )
    }
}
