//! Engine-owned canonical intent for selected or cluster-wide leader elections.

use core::mem::size_of;

use kafka_client_core::{
    ElectLeadersPlan, ElectLeadersPlanError, LeaderElectionTarget as CoreTarget,
    LeaderElectionType as CoreType,
};

/// Explicit leader-election policy independent of generated protocol types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaderElectionType {
    /// Elect the first eligible replica in each partition assignment.
    Preferred,
    /// Permit election of an out-of-sync replica when required.
    Unclean,
}

impl LeaderElectionType {
    const fn into_core(self) -> CoreType {
        match self {
            Self::Preferred => CoreType::Preferred,
            Self::Unclean => CoreType::Unclean,
        }
    }
}

/// One caller-ordered topic-partition selected for leader election.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderElectionTarget {
    topic: String,
    partition: i32,
}

impl LeaderElectionTarget {
    /// Creates one inert target for validation at submission.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    fn canonicalize(&mut self) {
        self.topic = canonical_string(core::mem::take(&mut self.topic));
    }

    fn into_core(self) -> CoreTarget {
        CoreTarget::new(self.topic, self.partition)
    }
}

/// Explicit engine request selection without empty-batch inference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersRequestSelection {
    /// Elect leaders for every partition in the cluster.
    AllPartitions,
    /// Elect leaders for one exact selected target batch.
    Selected(Vec<LeaderElectionTarget>),
}

/// One explicit election policy and partition selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectLeadersRequest {
    election_type: LeaderElectionType,
    selection: ElectLeadersRequestSelection,
}

impl ElectLeadersRequest {
    /// Creates selected-partition intent retained for source compatibility.
    pub const fn new(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Self {
        Self::selected(election_type, targets)
    }

    /// Creates inert selected-partition intent for validation at the call boundary.
    pub const fn selected(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Self {
        Self {
            election_type,
            selection: ElectLeadersRequestSelection::Selected(targets),
        }
    }

    /// Creates inert cluster-wide intent for validation at the call boundary.
    pub const fn all(election_type: LeaderElectionType) -> Self {
        Self {
            election_type,
            selection: ElectLeadersRequestSelection::AllPartitions,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        if let ElectLeadersRequestSelection::Selected(targets) = &mut self.selection {
            targets
                .iter_mut()
                .for_each(LeaderElectionTarget::canonicalize);
            targets.shrink_to_fit();
        }
        self
    }

    pub(crate) fn preparation_charge(&self) -> Option<usize> {
        match &self.selection {
            ElectLeadersRequestSelection::AllPartitions => Some(size_of::<Self>()),
            ElectLeadersRequestSelection::Selected(targets) => targets.iter().try_fold(
                size_of::<Self>().checked_add(
                    targets
                        .len()
                        .checked_mul(size_of::<LeaderElectionTarget>())?,
                )?,
                |bytes, target| bytes.checked_add(target.topic.len()),
            ),
        }
    }

    pub(crate) fn into_plan(self) -> Result<ElectLeadersPlan, ElectLeadersPlanFailure> {
        match self.selection {
            ElectLeadersRequestSelection::AllPartitions => {
                Ok(ElectLeadersPlan::all(self.election_type.into_core()))
            }
            ElectLeadersRequestSelection::Selected(targets) => {
                let mut selected = Vec::new();
                selected
                    .try_reserve_exact(targets.len())
                    .map_err(|_error| ElectLeadersPlanFailure::RetainedBytes)?;
                selected.extend(targets.into_iter().map(LeaderElectionTarget::into_core));
                ElectLeadersPlan::selected(self.election_type.into_core(), selected)
                    .map_err(ElectLeadersPlanFailure::Invalid)
            }
        }
    }
}

/// Request conversion failure before atomic host admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersPlanFailure {
    /// Core rejected the selected target identities.
    Invalid(ElectLeadersPlanError),
    /// Canonical core ownership could not fit an allocation.
    RetainedBytes,
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
