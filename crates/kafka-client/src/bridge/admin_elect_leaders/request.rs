//! Inert public leader-election intent translated only at the engine boundary.

use kafka_client_engine::{
    ElectLeadersRequest as EngineRequest, LeaderElectionTarget as EngineTarget,
    LeaderElectionType as EngineType,
};

use crate::{LeaderElectionTarget, LeaderElectionType};

/// Linear request retained by the public builder before submission.
pub(crate) struct ElectLeadersAdminRequest {
    inner: EngineRequest,
}

impl ElectLeadersAdminRequest {
    pub(crate) fn new(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Self {
        Self {
            inner: EngineRequest::new(
                into_engine_type(election_type),
                targets.into_iter().map(into_engine_target).collect(),
            ),
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for ElectLeadersAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ElectLeadersAdminRequest")
            .finish_non_exhaustive()
    }
}

const fn into_engine_type(election_type: LeaderElectionType) -> EngineType {
    match election_type {
        LeaderElectionType::Preferred => EngineType::Preferred,
        LeaderElectionType::Unclean => EngineType::Unclean,
    }
}

fn into_engine_target(target: LeaderElectionTarget) -> EngineTarget {
    let (topic, partition) = target.into_parts();
    EngineTarget::new(topic, partition)
}
