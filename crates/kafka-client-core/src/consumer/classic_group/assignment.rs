//! Bounded deterministic non-rack classic Range assignment policy.

use crate::{GroupAssignmentPartition, MemberId, PartitionIndex, TopicId};

use super::{
    ClassicJoinMember, ClassicJoinMembers, JoinedMemberSlot, TopicPartitionCount,
    range_validation::validate_counts,
};

/// Maximum partitions retained by one member assignment.
pub(super) const MAX_CLASSIC_MEMBER_PARTITIONS: usize = 64;

/// Maximum topic-partitions retained across one complete Sync plan.
pub(super) const MAX_CLASSIC_ASSIGNMENT_PARTITIONS: usize = 4_096;

/// One member's deterministic, possibly empty Range assignment.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicMemberAssignment {
    slot: JoinedMemberSlot,
    partitions: Vec<GroupAssignmentPartition>,
}

impl ClassicMemberAssignment {
    /// Returns the engine correlation slot for the opaque Kafka member.
    pub const fn slot(&self) -> JoinedMemberSlot {
        self.slot
    }

    /// Borrows topic partitions ordered by topic identity and partition.
    pub fn partitions(&self) -> &[GroupAssignmentPartition] {
        &self.partitions
    }
}

/// Complete Sync plan ordered by Kafka member rank.
#[derive(Debug, Eq, PartialEq)]
pub struct ClassicAssignmentPlan {
    assignments: Vec<ClassicMemberAssignment>,
}

impl ClassicAssignmentPlan {
    pub(crate) const fn empty() -> Self {
        Self {
            assignments: Vec::new(),
        }
    }

    /// Computes Kafka's dynamic-member, non-rack Range plan from scalar facts.
    pub fn try_range(
        members: &ClassicJoinMembers,
        counts: &[TopicPartitionCount],
    ) -> Result<Self, ClassicAssignmentError> {
        validate_counts(members, counts)?;
        preflight_plan(members, counts)?;

        let mut assignments = Vec::new();
        assignments
            .try_reserve_exact(members.members().len())
            .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
        for member in members.members() {
            assignments.push(assign_member(member, members, counts)?);
        }
        Ok(Self { assignments })
    }

    /// Borrows all member assignments, including members assigned no partitions.
    pub fn entries(&self) -> &[ClassicMemberAssignment] {
        &self.assignments
    }

    /// Removes the complete linear plan for one Sync request translation.
    pub fn into_sync_assignments(self) -> Vec<ClassicMemberAssignment> {
        self.assignments
    }
}

/// Structural or capacity rejection before a Range plan exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicAssignmentError {
    /// Topic counts were not ordered by stable topic identity.
    OutOfOrderTopicCount,
    /// One topic count appeared more than once.
    DuplicateTopicCount(TopicId),
    /// A subscribed topic had no partition-count fact.
    MissingTopicCount(TopicId),
    /// A partition-count fact named a topic no member subscribed to.
    UnsubscribedTopicCount(TopicId),
    /// Kafka's signed partition index domain cannot represent this count.
    PartitionCountOutOfRange(TopicId),
    /// Aggregate assigned partitions exceeded the reviewed bound.
    AggregatePartitionLimit {
        /// Exact aggregate partition count.
        actual: usize,
    },
    /// One member assignment exceeded the local session bound.
    MemberPartitionLimit {
        /// Member whose local assignment exceeded the bound.
        member_id: MemberId,
        /// Exact local partition count.
        actual: usize,
    },
    /// Checked count or partition arithmetic overflowed.
    ArithmeticOverflow,
    /// Bounded plan storage could not be reserved.
    AllocationFailed,
}

fn preflight_plan(
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
) -> Result<(), ClassicAssignmentError> {
    let aggregate = counts.iter().try_fold(0_usize, |sum, count| {
        sum.checked_add(count.count() as usize)
            .ok_or(ClassicAssignmentError::ArithmeticOverflow)
    })?;
    if aggregate > MAX_CLASSIC_ASSIGNMENT_PARTITIONS {
        return Err(ClassicAssignmentError::AggregatePartitionLimit { actual: aggregate });
    }
    for member in members.members() {
        let count = assigned_count(member, members, counts)?;
        if count > MAX_CLASSIC_MEMBER_PARTITIONS {
            return Err(ClassicAssignmentError::MemberPartitionLimit {
                member_id: member.member_id(),
                actual: count,
            });
        }
    }
    Ok(())
}

fn assign_member(
    member: &ClassicJoinMember,
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
) -> Result<ClassicMemberAssignment, ClassicAssignmentError> {
    let capacity = assigned_count(member, members, counts)?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(capacity)
        .map_err(|_| ClassicAssignmentError::AllocationFailed)?;
    for count in counts {
        if member.subscription().topics().contains(&count.topic_id()) {
            append_topic_range(&mut partitions, member, members, *count)?;
        }
    }
    Ok(ClassicMemberAssignment {
        slot: member.slot(),
        partitions,
    })
}

fn assigned_count(
    member: &ClassicJoinMember,
    members: &ClassicJoinMembers,
    counts: &[TopicPartitionCount],
) -> Result<usize, ClassicAssignmentError> {
    let mut total = 0_usize;
    for count in counts {
        if member.subscription().topics().contains(&count.topic_id()) {
            let (subscriber_index, subscriber_count) =
                subscriber_position(member, members, *count)?;
            let partition_count = count.count() as usize;
            let base = partition_count / subscriber_count;
            let extra = usize::from(subscriber_index < partition_count % subscriber_count);
            total = total
                .checked_add(base)
                .and_then(|value| value.checked_add(extra))
                .ok_or(ClassicAssignmentError::ArithmeticOverflow)?;
        }
    }
    Ok(total)
}

fn append_topic_range(
    output: &mut Vec<GroupAssignmentPartition>,
    member: &ClassicJoinMember,
    members: &ClassicJoinMembers,
    count: TopicPartitionCount,
) -> Result<(), ClassicAssignmentError> {
    let (index, subscribers) = subscriber_position(member, members, count)?;
    let partition_count = count.count() as usize;
    let base = partition_count / subscribers;
    let remainder = partition_count % subscribers;
    let start = index
        .checked_mul(base)
        .and_then(|value| value.checked_add(index.min(remainder)))
        .ok_or(ClassicAssignmentError::ArithmeticOverflow)?;
    let length = base + usize::from(index < remainder);
    let end = start
        .checked_add(length)
        .ok_or(ClassicAssignmentError::ArithmeticOverflow)?;
    for raw in start..end {
        let index = u32::try_from(raw).map_err(|_| ClassicAssignmentError::ArithmeticOverflow)?;
        output.push(GroupAssignmentPartition::new(
            count.topic_id(),
            PartitionIndex::from_raw(index),
        ));
    }
    Ok(())
}

fn subscriber_position(
    member: &ClassicJoinMember,
    members: &ClassicJoinMembers,
    count: TopicPartitionCount,
) -> Result<(usize, usize), ClassicAssignmentError> {
    let mut position = None;
    let mut subscriber_count = 0_usize;
    for candidate in members.members() {
        if candidate
            .subscription()
            .topics()
            .contains(&count.topic_id())
        {
            if candidate.member_id() == member.member_id() {
                position = Some(subscriber_count);
            }
            subscriber_count += 1;
        }
    }
    position
        .map(|index| (index, subscriber_count))
        .ok_or(ClassicAssignmentError::ArithmeticOverflow)
}
