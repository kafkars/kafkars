//! Scalar assignment fences and ordered `OffsetFetch` position facts.

use core::{fmt, num::NonZeroI16};

use super::super::{
    classic_group::MembershipCycle,
    group_commit::{AssignmentGeneration, GroupAssignmentPartition, GroupId, MemberId},
    model::NextFetchOffset,
};

/// Exact membership and assignment identity owning one position bootstrap.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupPositionFence {
    group_id: GroupId,
    cycle: MembershipCycle,
    member_id: MemberId,
    assignment_generation: AssignmentGeneration,
}

impl GroupPositionFence {
    /// Creates one exact group, membership, member, and assignment fence.
    pub const fn new(
        group_id: GroupId,
        membership_cycle: MembershipCycle,
        member_id: MemberId,
        assignment_generation: AssignmentGeneration,
    ) -> Self {
        Self {
            group_id,
            cycle: membership_cycle,
            member_id,
            assignment_generation,
        }
    }

    /// Returns the stable engine-catalog group identity.
    pub const fn group_id(self) -> GroupId {
        self.group_id
    }

    /// Returns the membership cycle that installed the assignment.
    pub const fn membership_cycle(self) -> MembershipCycle {
        self.cycle
    }

    /// Returns the stable engine-catalog member identity.
    pub const fn member_id(self) -> MemberId {
        self.member_id
    }

    /// Returns the core-owned live-assignment generation.
    pub const fn assignment_generation(self) -> AssignmentGeneration {
        self.assignment_generation
    }
}

/// Exact signed Kafka rejection from `OffsetFetch`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupPositionBrokerError {
    code: NonZeroI16,
}

impl GroupPositionBrokerError {
    /// Retains one exact nonzero signed Kafka error code.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// One assigned partition's normalized committed-position result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionPartitionResult {
    /// Kafka returned the nonnegative next offset for Fetch.
    Committed(NextFetchOffset),
    /// Kafka reported that this partition has no committed offset.
    Missing,
    /// Kafka rejected this exact partition.
    Rejected(GroupPositionBrokerError),
}

/// One exactly correlated assigned topic-partition fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupPositionPartitionFact {
    partition: GroupAssignmentPartition,
    result: GroupPositionPartitionResult,
}

impl GroupPositionPartitionFact {
    /// Creates one committed next-position fact.
    pub const fn committed(
        partition: GroupAssignmentPartition,
        next_offset: NextFetchOffset,
    ) -> Self {
        Self {
            partition,
            result: GroupPositionPartitionResult::Committed(next_offset),
        }
    }

    /// Creates one explicit missing-offset fact.
    pub const fn missing(partition: GroupAssignmentPartition) -> Self {
        Self {
            partition,
            result: GroupPositionPartitionResult::Missing,
        }
    }

    /// Creates one exact partition-level broker rejection.
    pub const fn rejected(
        partition: GroupAssignmentPartition,
        error: GroupPositionBrokerError,
    ) -> Self {
        Self {
            partition,
            result: GroupPositionPartitionResult::Rejected(error),
        }
    }

    /// Returns the assigned topic-partition identity.
    pub const fn partition(self) -> GroupAssignmentPartition {
        self.partition
    }

    /// Returns the normalized position result.
    pub const fn result(self) -> GroupPositionPartitionResult {
        self.result
    }
}

/// One ordered `OffsetFetch` response and its nonnegative broker throttle.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBatch {
    throttle_time_ms: u32,
    facts: Vec<GroupPositionPartitionFact>,
}

impl GroupPositionBatch {
    /// Creates one protocol-normalized response in exact request order.
    pub const fn new(throttle_time_ms: u32, facts: Vec<GroupPositionPartitionFact>) -> Self {
        Self {
            throttle_time_ms,
            facts,
        }
    }

    /// Returns Kafka's throttle observation without scheduling it.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Borrows position facts in exact assigned-partition order.
    pub fn facts(&self) -> &[GroupPositionPartitionFact] {
        &self.facts
    }

    /// Recovers the throttle and ordered scalar facts.
    pub fn into_parts(self) -> (u32, Vec<GroupPositionPartitionFact>) {
        (self.throttle_time_ms, self.facts)
    }
}

/// Structural or allocation rejection before machine construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapBuildErrorKind {
    /// Exact request storage could not be reserved.
    AllocationFailed,
    /// One assigned topic-partition appeared more than once.
    DuplicatePartition(GroupAssignmentPartition),
    /// One assigned topic-partition sorted before its predecessor.
    OutOfOrder {
        /// Preceding assignment fact.
        previous: GroupAssignmentPartition,
        /// Out-of-order assignment fact.
        current: GroupAssignmentPartition,
    },
}

/// Construction rejection retaining the exact ordered assignment.
#[must_use = "rejected group position assignment must be recovered or deliberately released"]
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapBuildError {
    kind: GroupPositionBootstrapBuildErrorKind,
    partitions: Vec<GroupAssignmentPartition>,
}

impl GroupPositionBootstrapBuildError {
    pub(crate) const fn new(
        kind: GroupPositionBootstrapBuildErrorKind,
        partitions: Vec<GroupAssignmentPartition>,
    ) -> Self {
        Self { kind, partitions }
    }

    /// Returns the exact construction failure.
    pub const fn kind(&self) -> GroupPositionBootstrapBuildErrorKind {
        self.kind
    }

    /// Recovers the exact caller-owned assignment.
    pub fn into_partitions(self) -> Vec<GroupAssignmentPartition> {
        self.partitions
    }
}

impl fmt::Display for GroupPositionBootstrapBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group position bootstrap is invalid: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupPositionBootstrapBuildError {}
