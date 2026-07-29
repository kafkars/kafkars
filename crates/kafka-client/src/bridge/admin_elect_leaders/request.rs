//! Inert public leader-election selection translated at the engine boundary.

use kafka_client_engine::{
    ElectLeadersRequest as EngineRequest, LeaderElectionTarget as EngineTarget,
    LeaderElectionType as EngineType,
};

use crate::{LeaderElectionTarget, LeaderElectionType};

enum Selection {
    Selected(Vec<LeaderElectionTarget>),
    All,
}

/// Linear request retained by the public builder before submission.
pub(crate) struct ElectLeadersAdminRequest {
    election_type: LeaderElectionType,
    selection: Selection,
}

impl ElectLeadersAdminRequest {
    pub(crate) fn new(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Self {
        Self {
            election_type,
            selection: Selection::Selected(targets),
        }
    }

    pub(crate) const fn all(election_type: LeaderElectionType) -> Self {
        Self {
            election_type,
            selection: Selection::All,
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        let election_type = into_engine_type(self.election_type);
        match self.selection {
            Selection::Selected(targets) => EngineRequest::selected(
                election_type,
                targets.into_iter().map(into_engine_target).collect(),
            ),
            Selection::All => EngineRequest::all(election_type),
        }
    }
}

impl std::fmt::Debug for ElectLeadersAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ElectLeadersAdminRequest")
            .field("election_type", &self.election_type)
            .field(
                "selection",
                &match &self.selection {
                    Selection::Selected(_) => "Selected",
                    Selection::All => "All",
                },
            )
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
