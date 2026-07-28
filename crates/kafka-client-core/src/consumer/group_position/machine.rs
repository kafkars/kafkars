//! Unique owner of one assignment's `OffsetFetch` bootstrap lifecycle.

use crate::Deadline;

use super::super::group_commit::GroupAssignmentPartition;
use super::{
    GroupPositionBootstrapBuildError, GroupPositionBootstrapBuildErrorKind,
    GroupPositionBootstrapState, GroupPositionFence, GroupPositionMissingOffsetPolicy,
};

/// Deterministic owner for one assignment-fenced position bootstrap.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapMachine {
    pub(super) fence: GroupPositionFence,
    pub(super) deadline: Deadline,
    pub(super) expected: Vec<GroupAssignmentPartition>,
    pub(super) request_partitions: Vec<GroupAssignmentPartition>,
    pub(super) missing_offset_policy: GroupPositionMissingOffsetPolicy,
    pub(super) state: GroupPositionBootstrapState,
}

impl GroupPositionBootstrapMachine {
    /// Validates and retains one ordered assignment plus its sole request copy.
    pub fn try_new(
        fence: GroupPositionFence,
        deadline: Deadline,
        partitions: Vec<GroupAssignmentPartition>,
    ) -> Result<Self, GroupPositionBootstrapBuildError> {
        Self::try_new_with_policy(
            fence,
            deadline,
            partitions,
            GroupPositionMissingOffsetPolicy::Error,
        )
    }

    /// Validates and retains one assignment plus explicit missing-offset policy.
    pub fn try_new_with_policy(
        fence: GroupPositionFence,
        deadline: Deadline,
        partitions: Vec<GroupAssignmentPartition>,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
    ) -> Result<Self, GroupPositionBootstrapBuildError> {
        if let Err(kind) = validate_partitions(&partitions) {
            return Err(GroupPositionBootstrapBuildError::new(kind, partitions));
        }
        let mut request_partitions = Vec::new();
        if request_partitions
            .try_reserve_exact(partitions.len())
            .is_err()
        {
            return Err(GroupPositionBootstrapBuildError::new(
                GroupPositionBootstrapBuildErrorKind::AllocationFailed,
                partitions,
            ));
        }
        request_partitions.extend_from_slice(&partitions);
        Ok(Self {
            fence,
            deadline,
            expected: partitions,
            request_partitions,
            missing_offset_policy,
            state: GroupPositionBootstrapState::Ready,
        })
    }

    /// Returns the exact assignment fence retained through settlement.
    pub const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    /// Returns the original absolute bootstrap deadline.
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Borrows the strict ordered assigned topic-partitions.
    pub fn partitions(&self) -> &[GroupAssignmentPartition] {
        &self.expected
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> GroupPositionBootstrapState {
        self.state
    }

    /// Returns actual retained correlation storage for engine accounting.
    pub fn expected_capacity(&self) -> usize {
        self.expected.capacity()
    }

    /// Returns the immutable missing-offset policy for this assignment.
    pub const fn missing_offset_policy(&self) -> GroupPositionMissingOffsetPolicy {
        self.missing_offset_policy
    }
}

fn validate_partitions(
    partitions: &[GroupAssignmentPartition],
) -> Result<(), GroupPositionBootstrapBuildErrorKind> {
    for pair in partitions.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if current == previous {
            return Err(GroupPositionBootstrapBuildErrorKind::DuplicatePartition(
                current,
            ));
        }
        if current < previous {
            return Err(GroupPositionBootstrapBuildErrorKind::OutOfOrder { previous, current });
        }
    }
    Ok(())
}
