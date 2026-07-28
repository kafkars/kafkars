//! Validated caller-ordered intent for one explicit leader-election type.

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
    targets: Vec<LeaderElectionTarget>,
}

impl ElectLeadersPlan {
    /// Validates one nonempty, caller-ordered unique target set.
    pub fn new(
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
            targets,
        })
    }

    /// Returns the explicit election policy.
    pub const fn election_type(&self) -> LeaderElectionType {
        self.election_type
    }

    /// Returns targets in exact caller order.
    pub fn targets(&self) -> &[LeaderElectionTarget] {
        &self.targets
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
