//! Unique deterministic owner for sequential group-position reset.

use crate::{Deadline, StartPosition, consumer::group_commit::GroupAssignmentPartition};

use super::super::{
    GroupPositionBatch, GroupPositionFence, GroupPositionMissingOffsetReset,
    GroupPositionPartitionResult,
};
use super::GroupPositionResetState;

/// Deterministic owner resolving missing offsets one partition at a time.
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionResetMachine {
    pub(super) fence: GroupPositionFence,
    pub(super) deadline: Deadline,
    pub(super) batch: Option<GroupPositionBatch>,
    pub(super) position: StartPosition,
    pub(super) current_missing_index: usize,
    pub(super) state: GroupPositionResetState,
}

impl GroupPositionResetMachine {
    /// Retains one exact reset terminal from the completed bootstrap.
    pub fn new(
        fence: GroupPositionFence,
        deadline: Deadline,
        reset: GroupPositionMissingOffsetReset,
    ) -> Self {
        let (batch, position) = reset.into_parts();
        let current_missing_index = batch
            .facts()
            .iter()
            .position(|fact| fact.result() == GroupPositionPartitionResult::Missing)
            .unwrap_or(batch.facts().len());
        Self {
            fence,
            deadline,
            batch: Some(batch),
            position,
            current_missing_index,
            state: GroupPositionResetState::Ready,
        }
    }

    /// Returns the exact assignment fence retained through settlement.
    pub const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    /// Returns the original absolute bootstrap deadline.
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Returns the current lifecycle stage.
    pub const fn state(&self) -> GroupPositionResetState {
        self.state
    }

    /// Returns the exact currently selected missing partition.
    pub fn current_partition(&self) -> Option<GroupAssignmentPartition> {
        self.batch
            .as_ref()
            .and_then(|batch| batch.facts().get(self.current_missing_index))
            .map(|fact| fact.partition())
    }
}
