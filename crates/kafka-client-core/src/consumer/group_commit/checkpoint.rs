//! Ordered scalar next-offset ownership for one exact group assignment.

use core::fmt;

use crate::{PartitionIndex, TopicId};

use super::{AssignmentGeneration, GroupId, MemberId};

/// One validated next offset and optional leader epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupCheckpointEntry {
    topic_id: TopicId,
    partition: PartitionIndex,
    next_offset: i64,
    leader_epoch: Option<i32>,
}

impl GroupCheckpointEntry {
    /// Validates one bytes-free topic-partition checkpoint entry.
    pub const fn try_new(
        topic_id: TopicId,
        partition: PartitionIndex,
        next_offset: i64,
        leader_epoch: Option<i32>,
    ) -> Result<Self, GroupCheckpointEntryError> {
        if next_offset < 0 {
            return Err(GroupCheckpointEntryError::NegativeNextOffset { value: next_offset });
        }
        if let Some(value) = leader_epoch
            && value < 0
        {
            return Err(GroupCheckpointEntryError::NegativeLeaderEpoch { value });
        }
        Ok(Self {
            topic_id,
            partition,
            next_offset,
            leader_epoch,
        })
    }

    /// Returns the engine-catalog topic identity.
    pub const fn topic_id(self) -> TopicId {
        self.topic_id
    }

    /// Returns the zero-based Kafka partition.
    pub const fn partition(self) -> PartitionIndex {
        self.partition
    }

    /// Returns the next offset to consume.
    pub const fn next_offset(self) -> i64 {
        self.next_offset
    }

    /// Returns the nonnegative leader epoch when one was observed.
    pub const fn leader_epoch(self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Scalar validation failure for one checkpoint entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupCheckpointEntryError {
    /// The next offset was below zero.
    NegativeNextOffset {
        /// Rejected offset.
        value: i64,
    },
    /// The supplied leader epoch was below zero.
    NegativeLeaderEpoch {
        /// Rejected epoch.
        value: i32,
    },
}

impl fmt::Display for GroupCheckpointEntryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeNextOffset { value } => {
                write!(formatter, "next offset {value} is negative")
            }
            Self::NegativeLeaderEpoch { value } => {
                write!(formatter, "leader epoch {value} is negative")
            }
        }
    }
}

impl std::error::Error for GroupCheckpointEntryError {}

/// Linear ordered checkpoint bound to one exact group assignment.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupCheckpoint {
    group_id: GroupId,
    member_id: MemberId,
    assignment_generation: AssignmentGeneration,
    entries: Vec<GroupCheckpointEntry>,
}

impl GroupCheckpoint {
    /// Validates nonempty strictly ordered unique topic-partition entries.
    pub fn try_new(
        group_id: GroupId,
        member_id: MemberId,
        assignment_generation: AssignmentGeneration,
        entries: Vec<GroupCheckpointEntry>,
    ) -> Result<Self, GroupCheckpointError> {
        let Some(first) = entries.first() else {
            return Err(GroupCheckpointError::Empty);
        };
        let mut previous = (first.topic_id(), first.partition());
        for entry in &entries[1..] {
            let current = (entry.topic_id(), entry.partition());
            if current == previous {
                return Err(GroupCheckpointError::DuplicateTopicPartition {
                    topic_id: current.0,
                    partition: current.1,
                });
            }
            if current < previous {
                return Err(GroupCheckpointError::OutOfOrder {
                    previous_topic_id: previous.0,
                    previous_partition: previous.1,
                    topic_id: current.0,
                    partition: current.1,
                });
            }
            previous = current;
        }
        Ok(Self {
            group_id,
            member_id,
            assignment_generation,
            entries,
        })
    }

    /// Returns the group catalog identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the member catalog identity.
    pub const fn member_id(&self) -> MemberId {
        self.member_id
    }

    /// Returns the assignment generation fencing every entry.
    pub const fn assignment_generation(&self) -> AssignmentGeneration {
        self.assignment_generation
    }

    /// Borrows the validated ordered entries.
    pub fn entries(&self) -> &[GroupCheckpointEntry] {
        &self.entries
    }
}

/// Structural validation failure for a group checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupCheckpointError {
    /// No topic-partition entry was supplied.
    Empty,
    /// One topic-partition appeared more than once.
    DuplicateTopicPartition {
        /// Repeated topic identity.
        topic_id: TopicId,
        /// Repeated partition.
        partition: PartitionIndex,
    },
    /// One topic-partition sorted before its predecessor.
    OutOfOrder {
        /// Preceding topic identity.
        previous_topic_id: TopicId,
        /// Preceding partition.
        previous_partition: PartitionIndex,
        /// Out-of-order topic identity.
        topic_id: TopicId,
        /// Out-of-order partition.
        partition: PartitionIndex,
    },
}

impl fmt::Display for GroupCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("group checkpoint is empty"),
            Self::DuplicateTopicPartition {
                topic_id,
                partition,
            } => write!(
                formatter,
                "group checkpoint repeats topic {} partition {}",
                topic_id.get(),
                partition.get()
            ),
            Self::OutOfOrder {
                previous_topic_id,
                previous_partition,
                topic_id,
                partition,
            } => write!(
                formatter,
                "topic {} partition {} follows topic {} partition {} out of order",
                topic_id.get(),
                partition.get(),
                previous_topic_id.get(),
                previous_partition.get()
            ),
        }
    }
}

impl std::error::Error for GroupCheckpointError {}
