//! Stable Rust vocabulary for current classic-group assignment state.

use crate::bridge::consumer_facade::group_consumer_metadata::{
    GroupConsumerMembershipEpoch as BridgeMembershipEpoch, GroupConsumerMetadata as BridgeMetadata,
};

/// One named topic-partition in a confirmed assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerAssignmentPartition {
    topic: String,
    partition: i32,
}

impl ConsumerAssignmentPartition {
    pub(crate) const fn from_parts(topic: String, partition: i32) -> Self {
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerAssignment {
    assignment_epoch: u64,
    partitions: Box<[ConsumerAssignmentPartition]>,
}

/// Broker-issued fencing epoch for one current group membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupMembershipEpoch {
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

impl From<BridgeMembershipEpoch> for GroupMembershipEpoch {
    fn from(epoch: BridgeMembershipEpoch) -> Self {
        match epoch {
            BridgeMembershipEpoch::Classic { generation_id } => Self::Classic { generation_id },
            BridgeMembershipEpoch::Consumer { member_epoch } => Self::Consumer { member_epoch },
        }
    }
}

impl GroupMembershipEpoch {
    /// Returns the classic generation, or `None` for KIP-848 membership.
    pub const fn classic_generation_id(self) -> Option<i32> {
        match self {
            Self::Classic { generation_id } => Some(generation_id),
            Self::Consumer { .. } => None,
        }
    }

    /// Returns the KIP-848 member epoch, or `None` for classic membership.
    pub const fn consumer_member_epoch(self) -> Option<i32> {
        match self {
            Self::Classic { .. } => None,
            Self::Consumer { member_epoch } => Some(member_epoch),
        }
    }
}

/// Stable identity of one current, confirmed group membership.
///
/// This value is suitable for fencing a future transactional offset transfer
/// against the same assignment generation. It is unavailable while joining or
/// after assignment loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupMetadata {
    group_id: String,
    member_id: String,
    membership_epoch: GroupMembershipEpoch,
    assignment_epoch: u64,
    group_instance_id: Option<String>,
    transaction_metadata: Option<BridgeMetadata>,
}

impl GroupMetadata {
    #[cfg(test)]
    pub(crate) const fn from_parts(
        group_id: String,
        member_id: String,
        membership_epoch: GroupMembershipEpoch,
        assignment_epoch: u64,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id,
            member_id,
            membership_epoch,
            assignment_epoch,
            group_instance_id,
            transaction_metadata: None,
        }
    }

    pub(crate) fn from_bridge(inner: BridgeMetadata) -> Self {
        Self {
            group_id: inner.group().to_owned(),
            member_id: inner.member().to_owned(),
            membership_epoch: inner.membership_epoch().into(),
            assignment_epoch: inner.assignment_epoch(),
            group_instance_id: inner.group_instance_id().map(str::to_owned),
            transaction_metadata: Some(inner),
        }
    }

    pub(crate) fn bridge_clone(&self) -> Option<BridgeMetadata> {
        self.transaction_metadata.clone()
    }

    /// Returns the exact Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the broker-issued group member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the protocol-specific broker fencing epoch.
    pub const fn membership_epoch(&self) -> GroupMembershipEpoch {
        self.membership_epoch
    }

    /// Returns the nonreused local assignment fence current with this metadata.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }

    /// Returns the configured static member identity, when present.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
}

impl ConsumerAssignment {
    pub(crate) fn from_parts(
        assignment_epoch: u64,
        partitions: Vec<ConsumerAssignmentPartition>,
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

    /// Borrows the ordered unique assigned topic-partitions.
    pub fn partitions(&self) -> &[ConsumerAssignmentPartition] {
        &self.partitions
    }
}
