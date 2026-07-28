//! Owned named facts observed from confirmed classic-group membership.

use std::sync::Arc;

use kafka_client_core::GroupPositionFence;

/// One named topic-partition in a confirmed classic-group assignment.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupConsumerAssignmentPartition {
    topic: Arc<str>,
    partition: i32,
}

impl GroupConsumerAssignmentPartition {
    pub(in crate::consumer) const fn new(topic: Arc<str>, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the exact Kafka topic spelling.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }
}

/// One confirmed assignment fence and its ordered topic-partitions.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupConsumerAssignment {
    assignment_epoch: u64,
    partitions: Box<[GroupConsumerAssignmentPartition]>,
}

impl GroupConsumerAssignment {
    pub(in crate::consumer) fn new(
        assignment_epoch: u64,
        partitions: Vec<GroupConsumerAssignmentPartition>,
    ) -> Self {
        Self {
            assignment_epoch,
            partitions: partitions.into_boxed_slice(),
        }
    }

    /// Returns the nonreused local assignment fence.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }

    /// Borrows the ordered unique topic-partitions.
    pub fn partitions(&self) -> &[GroupConsumerAssignmentPartition] {
        &self.partitions
    }
}

/// Stable protocol identity for one confirmed classic-group membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupConsumerMetadata {
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    member: Arc<str>,
    generation_id: i32,
    assignment_epoch: u64,
    _position_fence: GroupPositionFence,
}

impl GroupConsumerMetadata {
    pub(crate) const fn new_with_group_instance_id(
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        member: Arc<str>,
        generation_id: i32,
        assignment_epoch: u64,
        position_fence: GroupPositionFence,
    ) -> Self {
        Self {
            group,
            group_instance_id,
            member,
            generation_id,
            assignment_epoch,
            _position_fence: position_fence,
        }
    }

    /// Returns the exact Kafka group spelling.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the configured stable classic-group member identity, when present.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the broker-issued classic-group member identity.
    pub fn member(&self) -> &str {
        &self.member
    }

    /// Returns the broker-issued classic-group generation.
    pub const fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Returns the local assignment fence current for this metadata.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }
}

/// One atomically observed confirmed membership and assignment.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupConsumerState {
    assignment: GroupConsumerAssignment,
    metadata: GroupConsumerMetadata,
}

impl GroupConsumerState {
    pub(in crate::consumer) const fn new(
        assignment: GroupConsumerAssignment,
        metadata: GroupConsumerMetadata,
    ) -> Self {
        Self {
            assignment,
            metadata,
        }
    }

    /// Borrows the current confirmed assignment.
    pub const fn assignment(&self) -> &GroupConsumerAssignment {
        &self.assignment
    }

    /// Borrows the current confirmed group metadata.
    pub const fn metadata(&self) -> &GroupConsumerMetadata {
        &self.metadata
    }

    /// Splits the atomically observed facts into their owned values.
    pub fn into_parts(self) -> (GroupConsumerAssignment, GroupConsumerMetadata) {
        (self.assignment, self.metadata)
    }
}
