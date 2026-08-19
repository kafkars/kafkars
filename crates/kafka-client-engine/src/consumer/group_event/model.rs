//! Owned named facts observed from confirmed group membership.

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

/// Broker-issued fencing epoch for one confirmed group membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerMembershipEpoch {
    /// Generation assigned by the classic `JoinGroup` protocol.
    Classic {
        /// Nonnegative generation assigned by `JoinGroup`.
        generation_id: i32,
    },
    /// Member epoch assigned by the KIP-848 consumer-group protocol.
    Consumer {
        /// Positive epoch assigned by `ConsumerGroupHeartbeat`.
        member_epoch: i32,
    },
}

impl GroupConsumerMembershipEpoch {
    pub(crate) const fn generation_id_or_member_epoch(self) -> i32 {
        match self {
            Self::Classic { generation_id } => generation_id,
            Self::Consumer { member_epoch } => member_epoch,
        }
    }
}

/// Stable protocol identity for one confirmed group membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupConsumerMetadata {
    group: Arc<str>,
    group_instance_id: Option<Arc<str>>,
    member: Arc<str>,
    transaction_group_instance_id: Option<Arc<str>>,
    membership_epoch: GroupConsumerMembershipEpoch,
    assignment_epoch: u64,
    position_fence: GroupPositionFence,
}

impl GroupConsumerMetadata {
    pub(crate) const fn new_with_group_instance_id(
        group: Arc<str>,
        group_instance_id: Option<Arc<str>>,
        member: Arc<str>,
        transaction_group_instance_id: Option<Arc<str>>,
        membership_epoch: GroupConsumerMembershipEpoch,
        assignment_epoch: u64,
        position_fence: GroupPositionFence,
    ) -> Self {
        Self {
            group,
            group_instance_id,
            member,
            transaction_group_instance_id,
            membership_epoch,
            assignment_epoch,
            position_fence,
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

    /// Returns the broker-issued group member identity.
    pub fn member(&self) -> &str {
        &self.member
    }

    /// Returns the protocol-specific broker fencing epoch.
    pub const fn membership_epoch(&self) -> GroupConsumerMembershipEpoch {
        self.membership_epoch
    }

    pub(crate) const fn generation_id_or_member_epoch(&self) -> i32 {
        self.membership_epoch.generation_id_or_member_epoch()
    }

    /// Returns the local assignment fence current for this metadata.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }

    pub(crate) fn group_arc(&self) -> Arc<str> {
        Arc::clone(&self.group)
    }

    pub(crate) fn member_arc(&self) -> Arc<str> {
        Arc::clone(&self.member)
    }

    pub(crate) fn group_instance_id_arc(&self) -> Option<Arc<str>> {
        self.transaction_group_instance_id.as_ref().map(Arc::clone)
    }

    pub(crate) const fn position_fence(&self) -> GroupPositionFence {
        self.position_fence
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

/// Application-visible classic-group assignment transition.
///
/// The bounded lifecycle retains the prior terminal transition followed by a
/// current assignment. An unobserved assignment may be superseded by revoked or
/// lost state for that exact epoch.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupConsumerEvent {
    /// A successful Sync response was installed and driver-confirmed.
    PartitionsAssigned(GroupConsumerAssignment),
    /// The named assignment entered bounded graceful release and may be
    /// completed by its exact assignment epoch.
    PartitionsRevoked(GroupConsumerAssignment),
    /// The named assignment was retired; its epoch and every older checkpoint
    /// are stale.
    PartitionsLost(GroupConsumerAssignment),
}
