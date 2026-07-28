//! Stable Rust vocabulary for current classic-group assignment state.

use crate::bridge::consumer_facade::group_consumer_metadata::GroupConsumerMetadata as BridgeMetadata;

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

/// Stable identity of one current, Sync-confirmed classic-group membership.
///
/// This value is suitable for fencing a future transactional offset transfer
/// against the same assignment generation. It is unavailable while joining or
/// after assignment loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupMetadata {
    group_id: String,
    member_id: String,
    generation_id: i32,
    assignment_epoch: u64,
    group_instance_id: Option<String>,
    _transaction_metadata: Option<BridgeMetadata>,
}

impl GroupMetadata {
    #[cfg(test)]
    pub(crate) const fn from_parts(
        group_id: String,
        member_id: String,
        generation_id: i32,
        assignment_epoch: u64,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id,
            member_id,
            generation_id,
            assignment_epoch,
            group_instance_id,
            _transaction_metadata: None,
        }
    }

    pub(crate) fn from_bridge(inner: BridgeMetadata) -> Self {
        Self {
            group_id: inner.group().to_owned(),
            member_id: inner.member().to_owned(),
            generation_id: inner.generation_id(),
            assignment_epoch: inner.assignment_epoch(),
            group_instance_id: inner.group_instance_id().map(str::to_owned),
            _transaction_metadata: Some(inner),
        }
    }

    /// Returns the exact Kafka group spelling.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the broker-issued classic member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the broker-issued classic generation.
    pub const fn generation_id(&self) -> i32 {
        self.generation_id
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
