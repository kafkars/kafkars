//! Validated semantic selection for one partition-reassignment query.

use core::fmt;
use std::collections::BTreeSet;

const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

/// One caller-ordered topic-partition whose active reassignment is requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentTarget {
    topic: String,
    partition: i32,
}

impl ListPartitionReassignmentTarget {
    /// Creates one target for validation by the enclosing plan.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// Explicit query mode; an empty selected batch never means all partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartitionReassignmentsSelection {
    /// One nonempty caller-ordered set of unique topic-partitions.
    Selected(Vec<ListPartitionReassignmentTarget>),
    /// Every active partition reassignment visible to the controller.
    AllActive,
}

/// Validated intent for one read-only reassignment query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsPlan {
    selection: ListPartitionReassignmentsSelection,
}

impl ListPartitionReassignmentsPlan {
    /// Validates a nonempty caller-ordered unique target set.
    pub fn selected(
        targets: Vec<ListPartitionReassignmentTarget>,
    ) -> Result<Self, ListPartitionReassignmentsPlanError> {
        if targets.is_empty() {
            return Err(ListPartitionReassignmentsPlanError::EmptyTargetBatch);
        }
        let mut identities = BTreeSet::new();
        for target in &targets {
            validate_target(target)?;
            if !identities.insert((target.topic.as_str(), target.partition)) {
                return Err(ListPartitionReassignmentsPlanError::DuplicateTopicPartition);
            }
        }
        Ok(Self {
            selection: ListPartitionReassignmentsSelection::Selected(targets),
        })
    }

    /// Selects every currently active reassignment explicitly.
    pub const fn all_active() -> Self {
        Self {
            selection: ListPartitionReassignmentsSelection::AllActive,
        }
    }

    /// Returns the exact validated selection mode.
    pub const fn selection(&self) -> &ListPartitionReassignmentsSelection {
        &self.selection
    }
}

fn validate_target(
    target: &ListPartitionReassignmentTarget,
) -> Result<(), ListPartitionReassignmentsPlanError> {
    if target.topic.is_empty() {
        return Err(ListPartitionReassignmentsPlanError::EmptyTopicName);
    }
    if target.topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(ListPartitionReassignmentsPlanError::TopicNameTooLong);
    }
    if target.partition < 0 {
        return Err(ListPartitionReassignmentsPlanError::NegativePartition);
    }
    Ok(())
}

/// Invalid deterministic reassignment-listing intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPartitionReassignmentsPlanError {
    /// Selected mode must contain at least one topic-partition.
    EmptyTargetBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// One selected request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
}

impl fmt::Display for ListPartitionReassignmentsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyTargetBatch => {
                "selected partition-reassignment query has no topic-partitions"
            }
            Self::EmptyTopicName => "partition-reassignment query topic is empty",
            Self::TopicNameTooLong => "partition-reassignment query topic is too long",
            Self::NegativePartition => "partition-reassignment query partition is negative",
            Self::DuplicateTopicPartition => {
                "partition-reassignment query contains a duplicate topic-partition"
            }
        })
    }
}

impl std::error::Error for ListPartitionReassignmentsPlanError {}
