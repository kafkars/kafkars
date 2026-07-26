//! Sole construction and state-mutation owner for one pending classic Join.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupErrorKind, ClassicGroupInput, Deadline, MembershipCycle, Moment,
};

use crate::clock::DeadlineCapture;

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_join::{ClassicGroupExecutionState, PreparedClassicGroupJoin},
    classic_group_owner::ClassicGroupOwner,
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
            | ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
            | ClassicGroupExecutionState::CloseFault { .. } => {
                return Ok(false);
            }
            ClassicGroupExecutionState::JoinDriverOwned(driver_owned)
            | ClassicGroupExecutionState::LeaderDeferred(driver_owned) => {
                return if owner.machine().group_id() == driver_owned.identity().group_id()
                    && owner.machine().active_cycle() == Some(driver_owned.identity().cycle())
                {
                    Ok(false)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::JoinHandoff(_) => {
                return Err(ClassicGroupExecutionError::HandoffIncomplete);
            }
            ClassicGroupExecutionState::PreparedJoin(prepared) => prepared.deadline(),
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

    pub(super) const fn next_deadline(&self) -> Option<Deadline> {
        match &self.classic_execution_state {
            ClassicGroupExecutionState::PreparedJoin(prepared) => Some(prepared.deadline().core()),
            ClassicGroupExecutionState::PreparedSync(prepared) => Some(prepared.deadline().core()),
            ClassicGroupExecutionState::Idle
            | ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::JoinDriverOwned(_)
            | ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::LeaderDeferred(_)
            | ClassicGroupExecutionState::SyncHandoff(_)
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_)
            | ClassicGroupExecutionState::CloseFault { .. } => None,
        }
    }

    pub(super) fn unsettled(&self) -> usize {
        usize::from(!matches!(
            &self.classic_execution_state,
            ClassicGroupExecutionState::Idle
        ))
    }

    pub(super) const fn is_idle(&self) -> bool {
        matches!(
            self.classic_execution_state,
            ClassicGroupExecutionState::Idle
        )
    }

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

    pub(super) const fn prepared_join(&self) -> Option<&PreparedClassicGroupJoin> {
        match &self.classic_execution_state {
            ClassicGroupExecutionState::PreparedJoin(prepared) => Some(prepared),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) const fn close_fault(
        &self,
    ) -> Option<(
        &kafka_client_core::LiveGroupAssignment,
        kafka_client_core::ClassicGeneration,
        ClassicGroupAssignmentPreparationFailureKind,
    )> {
        match &self.classic_execution_state {
            ClassicGroupExecutionState::CloseFault {
                revoke_assignment,
                revoke_generation,
                revoke_failure_kind,
            } => Some((revoke_assignment, *revoke_generation, *revoke_failure_kind)),
            _ => None,
        }
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
    Assignment(ClassicGroupAssignmentPreparationFailureKind),
    Core(ClassicGroupErrorKind),
    EntryFault,
}
