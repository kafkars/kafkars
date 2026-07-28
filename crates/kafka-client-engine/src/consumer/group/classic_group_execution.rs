//! Sole construction and state-mutation owner for one pending classic Join.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput, MembershipCycle, Moment,
};

use crate::clock::DeadlineCapture;

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_join::{ClassicGroupExecutionState, PreparedClassicGroupJoin},
    classic_group_owner::ClassicGroupOwner,
    classic_group_position::ClassicGroupPositionExecutionError,
};

/// Separate mechanism ownership for one deterministic membership cycle.
pub(super) struct ClassicGroupExecution {
    classic_execution_state: ClassicGroupExecutionState,
}

pub(super) const fn new_classic_group_execution() -> ClassicGroupExecution {
    ClassicGroupExecution {
        classic_execution_state: ClassicGroupExecutionState::Idle,
    }
}

impl ClassicGroupExecution {
    pub(super) fn begin(
        &mut self,
        owner: &mut ClassicGroupOwner,
        capture: DeadlineCapture,
    ) -> Result<MembershipCycle, ClassicGroupExecutionError> {
        if !matches!(
            &self.classic_execution_state,
            ClassicGroupExecutionState::Idle
        ) {
            return Err(ClassicGroupExecutionError::Occupied);
        }
        let transition = owner
            .apply(ClassicGroupInput::Begin {
                now: capture.now(),
                deadline: capture.deadline(),
            })
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        let mut effects = transition.into_effects();
        let prepared = match effects.next() {
            Some(ClassicGroupEffect::Join {
                group_id,
                cycle,
                protocol,
                member_id: None,
                timing,
                deadline,
            }) if group_id == owner.machine().group_id()
                && timing == owner.machine().timing()
                && deadline == capture.deadline()
                && effects.next().is_none() =>
            {
                PreparedClassicGroupJoin::new(
                    group_id,
                    cycle,
                    protocol,
                    timing,
                    capture.operation_deadline(),
                )
            }
            _ => return Err(ClassicGroupExecutionError::UnexpectedBeginEffect),
        };
        let cycle = prepared.cycle();
        self.classic_execution_state = ClassicGroupExecutionState::PreparedJoin(prepared);
        Ok(cycle)
    }

    pub(super) fn expire_if_due(
        &mut self,
        owner: &mut ClassicGroupOwner,
        now: Moment,
    ) -> Result<bool, ClassicGroupExecutionError> {
        let deadline = match &self.classic_execution_state {
            ClassicGroupExecutionState::Idle
            | ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::PartitionCountsPostCore { .. }
            | ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
            | ClassicGroupExecutionState::CloseFault { .. } => {
                return Ok(false);
            }
            ClassicGroupExecutionState::JoinDriverOwned(driver_owned) => {
                return if owner.machine().group_id() == driver_owned.identity().group_id()
                    && owner.machine().active_cycle() == Some(driver_owned.identity().cycle())
                {
                    Ok(false)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::PartitionCountDriverOwned { call, .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { call, .. } => {
                return if owner.machine().group_id() == call.identity().group_id()
                    && owner.machine().active_cycle() == Some(call.identity().cycle())
                {
                    Ok(false)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::PartitionCountHandoff { .. } => {
                return Err(ClassicGroupExecutionError::HandoffIncomplete);
            }
            ClassicGroupExecutionState::PreparedJoin(prepared) => prepared.deadline(),
            ClassicGroupExecutionState::PreparedPartitionCounts(prepared) => prepared.deadline(),
            ClassicGroupExecutionState::PreparedSync(prepared) => prepared.deadline(),
        };
        if !deadline.core().is_elapsed_at(now) {
            return Ok(false);
        }
        let cycle = owner
            .machine()
            .active_cycle()
            .ok_or(ClassicGroupExecutionError::MissingCycle)?;
        let transition = owner
            .apply(ClassicGroupInput::DeadlineElapsed { cycle, now })
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        if transition.into_effects().next().is_some() {
            return Err(ClassicGroupExecutionError::UnexpectedDeadlineEffect);
        }
        self.classic_execution_state = ClassicGroupExecutionState::Idle;
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "rejected rejoin staging returns the exact linear prepared Join without allocation"
    )]
    pub(super) fn stage_rejoin_join(
        &mut self,
        prepared: PreparedClassicGroupJoin,
    ) -> Result<(), (ClassicGroupExecutionError, PreparedClassicGroupJoin)> {
        if !self.is_idle() {
            return Err((ClassicGroupExecutionError::Occupied, prepared));
        }
        self.classic_execution_state = ClassicGroupExecutionState::PreparedJoin(prepared);
        Ok(())
    }

    pub(super) const fn borrow_execution_state(&self) -> &ClassicGroupExecutionState {
        &self.classic_execution_state
    }

    pub(super) fn replace_execution_state(
        &mut self,
        replacement: ClassicGroupExecutionState,
    ) -> ClassicGroupExecutionState {
        core::mem::replace(&mut self.classic_execution_state, replacement)
    }

    pub(super) fn set_execution_state(&mut self, state: ClassicGroupExecutionState) {
        self.classic_execution_state = state;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupExecutionError {
    Occupied,
    MissingCycle,
    JoinNotPrepared,
    SyncNotPrepared,
    CloseNotFaulted,
    HandoffIncomplete,
    HandoffMismatch,
    UnexpectedBeginEffect,
    UnexpectedCloseEffect,
    UnexpectedDeadlineEffect,
    JoinRequest,
    CallRegistryUnavailable,
    CallIdentityMismatch,
    CallCompletion,
    JoinTerminal,
    SyncTerminal,
    HeartbeatState,
    HeartbeatTerminal,
    RejoinState,
    RejoinPostCore,
    CoordinatorInvalidationReserve,
    CoordinatorInvalidationTransfer,
    CoordinatorInvalidationInstall,
    CoordinatorInvalidationGate,
    CoordinatorInvalidationAdmission,
    CoordinatorInvalidationTerminal,
    FollowerJoin,
    LeaderJoin,
    PartitionCountsNotPrepared,
    PartitionCountFence,
    PartitionCountTerminal,
    LeaderPartitionCounts,
    PartitionCountsPostCore,
    Assignment(ClassicGroupAssignmentPreparationFailureKind),
    ProcessingLease(kafka_client_core::ClassicProcessingLeaseError),
    FetchRetirement(super::classic_group_fetch::ClassicGroupFetchRetirementError),
    PositionPreparation,
    PositionCallsUnavailable,
    PositionPending,
    PositionDuplicateFence(kafka_client_core::GroupPositionFence),
    Position(ClassicGroupPositionExecutionError),
    Core(ClassicGroupErrorKind),
    EntryFault,
}
