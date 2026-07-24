//! Direct-assignment identities, start positions, and generation fences.

use crate::{PartitionIndex, TopicId};

/// Monotonic identity of one complete direct assignment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignmentEpoch(u64);

impl AssignmentEpoch {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the deterministic epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation fencing position replacement within one assigned partition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PositionEpoch(u64);

impl PositionEpoch {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the deterministic epoch value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic identity of one fetch issued for a position epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchRevision(u64);

impl FetchRevision {
    pub(crate) const fn initial() -> Self {
        Self(1)
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn try_from_raw_for_test(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    /// Returns the deterministic revision value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Engine-catalog topic identity paired with one validated partition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssignedTopicPartition {
    topic_id: TopicId,
    partition: PartitionIndex,
}

impl AssignedTopicPartition {
    /// Creates one explicit topic-partition identity.
    pub const fn new(topic_id: TopicId, partition: PartitionIndex) -> Self {
        Self {
            topic_id,
            partition,
        }
    }

    /// Returns the engine-catalog topic identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }
}

/// Validated next offset for one future fetch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NextFetchOffset(i64);

impl NextFetchOffset {
    /// Validates a nonnegative Kafka fetch offset.
    pub const fn try_from_raw(value: i64) -> Option<Self> {
        if value < 0 { None } else { Some(Self(value)) }
    }

    /// Returns the Kafka offset used by the next fetch.
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Explicit policy for resolving the first fetch position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartPosition {
    /// Resolve the earliest available offset.
    Beginning,
    /// Resolve the end offset observed by Kafka.
    End,
    /// Begin at one explicit next-fetch offset.
    Offset(NextFetchOffset),
}

/// One partition and its required initial position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedPartition {
    partition: AssignedTopicPartition,
    start: StartPosition,
}

impl AssignedPartition {
    /// Creates one direct-assignment entry.
    pub const fn new(partition: AssignedTopicPartition, start: StartPosition) -> Self {
        Self { partition, start }
    }

    /// Returns the assigned topic-partition.
    pub const fn partition(self) -> AssignedTopicPartition {
        self.partition
    }

    /// Returns the explicit initial-position policy.
    pub const fn start(self) -> StartPosition {
        self.start
    }
}

/// Exact assignment and position generation for one interpreter action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositionFence {
    assignment_epoch: AssignmentEpoch,
    partition: AssignedTopicPartition,
    position_epoch: PositionEpoch,
}

impl PositionFence {
    pub(crate) const fn new(
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        position_epoch: PositionEpoch,
    ) -> Self {
        Self {
            assignment_epoch,
            partition,
            position_epoch,
        }
    }

    /// Returns the complete assignment generation.
    pub const fn assignment_epoch(self) -> AssignmentEpoch {
        self.assignment_epoch
    }

    /// Returns the fenced topic-partition.
    pub const fn partition(self) -> AssignedTopicPartition {
        self.partition
    }

    /// Returns the partition-local position generation.
    pub const fn position_epoch(self) -> PositionEpoch {
        self.position_epoch
    }
}

/// Deterministic ownership of one prepared position-resolution fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionOwnership {
    /// The exact position lookup still owns the active resolution attempt.
    Active,
    /// A directionally newer assignment, position, or terminal superseded it.
    Superseded,
}

/// Exact identity of one future-engine fetch execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FetchFence {
    position: PositionFence,
    revision: FetchRevision,
}

/// Deterministic ownership of one prepared Fetch fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchOwnership {
    /// The exact Fetch still owns one active partition execution.
    Active,
    /// A directionally newer assignment, position, or Fetch superseded it.
    Superseded,
}

impl FetchFence {
    pub(crate) const fn new(position: PositionFence, revision: FetchRevision) -> Self {
        Self { position, revision }
    }

    /// Returns the assignment and position fence.
    pub const fn position(self) -> PositionFence {
        self.position
    }

    /// Returns the partition-local fetch revision.
    pub const fn revision(self) -> FetchRevision {
        self.revision
    }
}

/// Whether one normalized Fetch result retains records visible to the application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FetchRecords {
    /// The response was empty or contained only Kafka control records.
    NoApplicationRecords,
    /// The engine retains one or more application records for delivery.
    Deliverable,
}
