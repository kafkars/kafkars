//! Validated caller-ordered or cluster-wide leader-election intent.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// Kafka's two explicit partition leader-election policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderElectionType {
    /// Elect the first eligible replica in the partition assignment.
    Preferred,
    /// Elect an out-of-sync replica when no in-sync replica is available.
    Unclean,
}

/// One caller-ordered topic-partition selected for leader election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElectionTarget {
    topic: String,
    partition: i32,
}

impl LeaderElectionTarget {
    /// Creates one target for validation by the enclosing plan.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// Validated intent for one destructive controller request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersPlan {
    election_type: LeaderElectionType,
    selection: ElectLeadersSelection,
}

/// Explicit partition selection without conflating an empty batch with all partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectLeadersSelection {
    /// Elect leaders for every partition in the cluster.
    AllPartitions,
    /// Elect leaders for one validated nonempty caller-ordered target batch.
    Selected(Vec<LeaderElectionTarget>),
}

impl ElectLeadersSelection {
    /// Returns selected targets in caller order, or `None` for all partitions.
    pub fn selected_targets(&self) -> Option<&[LeaderElectionTarget]> {
        match self {
            Self::AllPartitions => None,
            Self::Selected(targets) => Some(targets),
        }
    }
}

impl ElectLeadersPlan {
    /// Validates one nonempty selected target batch.
    pub fn new(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Result<Self, ElectLeadersPlanError> {
        Self::selected(election_type, targets)
    }

    /// Validates one nonempty, caller-ordered unique selected target batch.
    pub fn selected(
        election_type: LeaderElectionType,
        targets: Vec<LeaderElectionTarget>,
    ) -> Result<Self, ElectLeadersPlanError> {
        if targets.is_empty() {
            return Err(ElectLeadersPlanError::EmptyBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(ElectLeadersPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self {
            election_type,
            selection: ElectLeadersSelection::Selected(targets),
        })
    }

    /// Creates one explicit cluster-wide election plan.
    pub const fn all(election_type: LeaderElectionType) -> Self {
        Self {
            election_type,
            selection: ElectLeadersSelection::AllPartitions,
        }
    }

    /// Returns the explicit election policy.
    pub const fn election_type(&self) -> LeaderElectionType {
        self.election_type
    }

    /// Returns the explicit all-partitions or selected-partition intent.
    pub const fn selection(&self) -> &ElectLeadersSelection {
        &self.selection
    }

    /// Returns selected targets in exact caller order.
    ///
    /// This compatibility accessor is empty for an all-partitions plan. New
    /// routing code must inspect [`Self::selection`] instead of inferring
    /// selection semantics from this slice.
    pub fn targets(&self) -> &[LeaderElectionTarget] {
        self.selection.selected_targets().unwrap_or(&[])
    }
}

fn validate_target(target: &LeaderElectionTarget) -> Result<(), ElectLeadersPlanError> {
    if target.topic.is_empty() {
        return Err(ElectLeadersPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(ElectLeadersPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(ElectLeadersPlanError::NegativePartition);
    }
    Ok(())
}

/// Invalid deterministic leader-election intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectLeadersPlanError {
    /// The operation must carry at least one target.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names must fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// One request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for ElectLeadersPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "leader-election target batch is empty",
            Self::EmptyTopicName => "leader-election topic is empty",
            Self::TopicNameTooLong => "leader-election topic is too long",
            Self::NegativePartition => "leader-election partition is negative",
            Self::DuplicateTopicPartition => "leader-election request repeats a topic-partition",
        })
    }
}

impl std::error::Error for ElectLeadersPlanError {}
