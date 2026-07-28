//! Validated caller-ordered partition-reassignment alteration intent.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// Replacement replica placement or explicit cancellation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionReassignmentTarget {
    /// Replace the current assignment with this ordered broker list.
    Replicas(Vec<i32>),
    /// Cancel an in-progress reassignment for this partition.
    Cancel,
}

impl PartitionReassignmentTarget {
    /// Returns the replacement brokers, or `None` for explicit cancellation.
    pub fn replicas(&self) -> Option<&[i32]> {
        match self {
            Self::Replicas(replicas) => Some(replicas),
            Self::Cancel => None,
        }
    }
}

/// One caller-ordered topic-partition reassignment change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignment {
    topic: String,
    partition: i32,
    target: PartitionReassignmentTarget,
}

impl AlterPartitionReassignment {
    /// Creates one change for validation by the enclosing request plan.
    pub const fn new(topic: String, partition: i32, target: PartitionReassignmentTarget) -> Self {
        Self {
            topic,
            partition,
            target,
        }
    }

    /// Returns the exact topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the requested replacement or explicit cancellation.
    pub const fn target(&self) -> &PartitionReassignmentTarget {
        &self.target
    }
}

/// Validated intent for one destructive controller request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsPlan {
    changes: Vec<AlterPartitionReassignment>,
    allow_replication_factor_change: bool,
}

impl AlterPartitionReassignmentsPlan {
    /// Validates one nonempty, caller-ordered unique change set.
    pub fn new(
        changes: Vec<AlterPartitionReassignment>,
    ) -> Result<Self, AlterPartitionReassignmentsPlanError> {
        if changes.is_empty() {
            return Err(AlterPartitionReassignmentsPlanError::EmptyBatch);
        }
        let mut identities = BTreeSet::new();
        for change in &changes {
            validate_change(change)?;
            if !identities.insert((change.topic.as_str(), change.partition)) {
                return Err(AlterPartitionReassignmentsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self {
            changes,
            allow_replication_factor_change: true,
        })
    }

    /// Returns changes in exact caller order.
    pub fn changes(&self) -> &[AlterPartitionReassignment] {
        &self.changes
    }

    /// Replaces whether Kafka may change a partition's replication factor.
    pub const fn with_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }

    /// Returns whether Kafka may change a partition's replication factor.
    pub const fn allow_replication_factor_change(&self) -> bool {
        self.allow_replication_factor_change
    }
}

fn validate_change(
    change: &AlterPartitionReassignment,
) -> Result<(), AlterPartitionReassignmentsPlanError> {
    if change.topic.is_empty() {
        return Err(AlterPartitionReassignmentsPlanError::EmptyTopicName);
    }
    if change.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(AlterPartitionReassignmentsPlanError::TopicNameTooLong);
    }
    if change.partition < 0 {
        return Err(AlterPartitionReassignmentsPlanError::NegativePartition);
    }
    let PartitionReassignmentTarget::Replicas(replicas) = &change.target else {
        return Ok(());
    };
    if replicas.is_empty() {
        return Err(AlterPartitionReassignmentsPlanError::EmptyReplicaList);
    }
    let mut broker_ids = BTreeSet::new();
    for broker_id in replicas {
        if *broker_id < 0 {
            return Err(AlterPartitionReassignmentsPlanError::NegativeBrokerId);
        }
        if !broker_ids.insert(*broker_id) {
            return Err(AlterPartitionReassignmentsPlanError::DuplicateBrokerId);
        }
    }
    Ok(())
}

/// Invalid deterministic reassignment alteration intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterPartitionReassignmentsPlanError {
    /// The operation must carry at least one change.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names must fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// A replacement must name at least one replica.
    EmptyReplicaList,
    /// Broker IDs must be nonnegative.
    NegativeBrokerId,
    /// One replacement cannot repeat a broker ID.
    DuplicateBrokerId,
    /// One request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for AlterPartitionReassignmentsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "partition reassignment alteration batch is empty",
            Self::EmptyTopicName => "partition reassignment topic is empty",
            Self::TopicNameTooLong => "partition reassignment topic is too long",
            Self::NegativePartition => "partition reassignment partition is negative",
            Self::EmptyReplicaList => "partition reassignment replica list is empty",
            Self::NegativeBrokerId => "partition reassignment broker id is negative",
            Self::DuplicateBrokerId => "partition reassignment repeats a broker id",
            Self::DuplicateTopicPartition => {
                "partition reassignment alteration repeats a topic-partition"
            }
        })
    }
}

impl std::error::Error for AlterPartitionReassignmentsPlanError {}
