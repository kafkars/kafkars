//! Engine-owned canonical intent for selected partition leader elections.

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

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self
    }

    fn into_core(self) -> CoreTarget {
        CoreTarget::new(self.topic, self.partition)
    }
}

/// One explicit election policy and nonempty caller-ordered target batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectLeadersRequest {
    election_type: LeaderElectionType,
    targets: Vec<LeaderElectionTarget>,
}

impl ElectLeadersRequest {
    /// Creates one inert request for validation at the public call boundary.
    pub const fn new(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Self {
        Self {
            election_type,
            targets,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.targets = canonical_vec(
            self.targets
                .into_iter()
                .map(LeaderElectionTarget::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn preparation_charge(&self) -> Option<usize> {
        self.targets.iter().try_fold(
            size_of::<Self>().checked_add(
                self.targets
                    .len()
                    .checked_mul(size_of::<LeaderElectionTarget>())?,
            )?,
            |bytes, target| bytes.checked_add(target.topic.len()),
        )
    }

    pub(crate) fn into_plan(self) -> Result<ElectLeadersPlan, ElectLeadersPlanError> {
        ElectLeadersPlan::new(
            self.election_type.into_core(),
            self.targets
                .into_iter()
                .map(LeaderElectionTarget::into_core)
                .collect(),
        )
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
