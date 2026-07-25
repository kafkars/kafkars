//! Validated bytes-free facts for one currently live group assignment.

use core::fmt;

use crate::{PartitionIndex, TopicId};

use super::{AssignmentGeneration, GroupCheckpoint, GroupId, MemberId};

/// One topic-partition owned by a live group assignment.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GroupAssignmentPartition {
    topic_id: TopicId,
    partition: PartitionIndex,
}

impl GroupAssignmentPartition {
    /// Creates one scalar assignment partition.
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

/// One validated current group/member/generation and its assigned partitions.
#[derive(Debug, Eq, PartialEq)]
pub struct LiveGroupAssignment {
    group_id: GroupId,
    member_id: MemberId,
    assignment_generation: AssignmentGeneration,
    partitions: Vec<GroupAssignmentPartition>,
}

impl LiveGroupAssignment {
    /// Validates a strictly ordered unique partition set for one live assignment.
    pub fn try_new(
        group_id: GroupId,
        member_id: MemberId,
        assignment_generation: AssignmentGeneration,
        partitions: Vec<GroupAssignmentPartition>,
    ) -> Result<Self, LiveGroupAssignmentError> {
        for pair in partitions.windows(2) {
            let previous = pair[0];
            let current = pair[1];
            if current == previous {
                return Err(LiveGroupAssignmentError::DuplicatePartition {
                    topic_id: current.topic_id(),
                    partition: current.partition(),
                });
            }
            if current < previous {
                return Err(LiveGroupAssignmentError::OutOfOrder { previous, current });
            }
        }
        Ok(Self {
            group_id,
            member_id,
            assignment_generation,
            partitions,
        })
    }

    /// Returns the current group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the current member identity.
    pub const fn member_id(&self) -> MemberId {
        self.member_id
    }

    /// Returns the current assignment generation or member epoch.
    pub const fn assignment_generation(&self) -> AssignmentGeneration {
        self.assignment_generation
    }

    /// Borrows the ordered unique partition set.
    pub fn partitions(&self) -> &[GroupAssignmentPartition] {
        &self.partitions
    }

    /// Returns actual retained partition-vector capacity for engine accounting.
    pub fn partitions_capacity(&self) -> usize {
        self.partitions.capacity()
    }

    pub(crate) fn contains(&self, partition: GroupAssignmentPartition) -> bool {
        self.partitions.binary_search(&partition).is_ok()
    }

    pub(crate) fn validate_checkpoint(
        &self,
        checkpoint: &GroupCheckpoint,
    ) -> Result<(), GroupOffsetCommitAdmissionErrorKind> {
        if checkpoint.group_id() != self.group_id {
            return Err(GroupOffsetCommitAdmissionErrorKind::GroupMismatch);
        }
        if checkpoint.member_id() != self.member_id {
            return Err(GroupOffsetCommitAdmissionErrorKind::MemberMismatch);
        }
        if checkpoint.assignment_generation() != self.assignment_generation {
            return Err(GroupOffsetCommitAdmissionErrorKind::GenerationMismatch);
        }
        if let Some(partition) = checkpoint
            .entries()
            .iter()
            .map(|entry| GroupAssignmentPartition::new(entry.topic_id(), entry.partition()))
            .find(|partition| !self.contains(*partition))
        {
            return Err(GroupOffsetCommitAdmissionErrorKind::UnassignedPartition {
                topic_id: partition.topic_id(),
                partition: partition.partition(),
            });
        }
        Ok(())
    }
}

/// Structural rejection of one live-assignment fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveGroupAssignmentError {
    /// One topic-partition appeared more than once.
    DuplicatePartition {
        /// Repeated topic identity.
        topic_id: TopicId,
        /// Repeated partition.
        partition: PartitionIndex,
    },
    /// One topic-partition sorted before its predecessor.
    OutOfOrder {
        /// Preceding partition.
        previous: GroupAssignmentPartition,
        /// Out-of-order partition.
        current: GroupAssignmentPartition,
    },
}

impl fmt::Display for LiveGroupAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "live group assignment is invalid: {self:?}")
    }
}

impl std::error::Error for LiveGroupAssignmentError {}

/// Local stale/lost assignment rejection before commit admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupOffsetCommitAdmissionErrorKind {
    /// No assignment is currently live.
    AssignmentLost,
    /// Core could not reserve exact correlation storage.
    AllocationFailed,
    /// The checkpoint belongs to another group.
    GroupMismatch,
    /// The checkpoint belongs to another member.
    MemberMismatch,
    /// The checkpoint belongs to a superseded generation.
    GenerationMismatch,
    /// The checkpoint contains a partition outside the live assignment.
    UnassignedPartition {
        /// Unassigned topic identity.
        topic_id: TopicId,
        /// Unassigned partition.
        partition: PartitionIndex,
    },
}

/// Local admission rejection retaining the exact linear checkpoint.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupOffsetCommitAdmissionError {
    kind: GroupOffsetCommitAdmissionErrorKind,
    checkpoint: GroupCheckpoint,
}

impl GroupOffsetCommitAdmissionError {
    pub(crate) const fn new(
        kind: GroupOffsetCommitAdmissionErrorKind,
        checkpoint: GroupCheckpoint,
    ) -> Self {
        Self { kind, checkpoint }
    }

    /// Returns the local assignment rejection reason.
    pub const fn kind(&self) -> GroupOffsetCommitAdmissionErrorKind {
        self.kind
    }

    /// Borrows the rejected checkpoint.
    pub const fn checkpoint(&self) -> &GroupCheckpoint {
        &self.checkpoint
    }

    /// Recovers the exact rejected linear checkpoint.
    pub fn into_checkpoint(self) -> GroupCheckpoint {
        self.checkpoint
    }
}

impl fmt::Display for GroupOffsetCommitAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group offset commit admission rejected checkpoint: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupOffsetCommitAdmissionError {}

pub(super) fn reserve_expected_partitions(
    expected: &mut Vec<GroupAssignmentPartition>,
    count: usize,
) -> bool {
    expected.try_reserve_exact(count).is_ok()
}
