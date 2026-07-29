//! Selected and cluster-wide leader-election entry points on the admin handle.

use crate::{
    admin::{ElectLeadersBuilder, LeaderElectionTarget, LeaderElectionType},
    bridge::admin_elect_leaders::ElectLeadersAdminRequest,
};

use super::Admin;

impl Admin {
    /// Builds an inert caller-ordered selected-partition leader election.
    pub fn elect_leaders<I>(
        &self,
        election_type: LeaderElectionType,
        targets: I,
    ) -> ElectLeadersBuilder
    where
        I: IntoIterator<Item = LeaderElectionTarget>,
    {
        ElectLeadersBuilder::new(
            self.engine.clone(),
            ElectLeadersAdminRequest::new(election_type, targets.into_iter().collect()),
            self.engine.default_timeout(),
        )
    }

    /// Builds an inert cluster-wide leader election.
    ///
    /// This explicit all-partitions selection is distinct from passing an
    /// empty iterator to [`Self::elect_leaders`], which remains invalid.
    pub fn elect_all_leaders(&self, election_type: LeaderElectionType) -> ElectLeadersBuilder {
        ElectLeadersBuilder::new(
            self.engine.clone(),
            ElectLeadersAdminRequest::all(election_type),
            self.engine.default_timeout(),
        )
    }
}
