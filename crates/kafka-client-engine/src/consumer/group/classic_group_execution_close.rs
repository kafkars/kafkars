//! Local close policy using the execution owner's guarded state operations.

use kafka_client_core::{
    ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, ClassicProcessingLease,
};

use super::{
    classic_group_assignment::retire_and_revoke_classic_group_assignment,
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_fetch::ClassicGroupFetchOwner,
    classic_group_heartbeat_prepare::map_revocation_kind,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_owner::ClassicGroupOwner,
    session_catalog::GroupSessionCatalog,
};

impl ClassicGroupExecution {
    pub(super) fn close_if_local(
        &mut self,
        owner: &mut ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
    ) -> Result<ClassicGroupCloseProgress, ClassicGroupExecutionError> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::JoinDriverOwned(driver_owned) => {
                return if owner.machine().group_id() == driver_owned.identity().group_id()
                    && owner.machine().active_cycle() == Some(driver_owned.identity().cycle())
                {
                    Ok(ClassicGroupCloseProgress::DriverOwned)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::PartitionCountDriverOwned { call, .. }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { call, .. } => {
                return if owner.machine().group_id() == call.identity().group_id()
                    && owner.machine().active_cycle() == Some(call.identity().cycle())
                {
                    Ok(ClassicGroupCloseProgress::DriverOwned)
                } else {
                    Err(ClassicGroupExecutionError::HandoffMismatch)
                };
            }
            ClassicGroupExecutionState::JoinHandoff(_)
            | ClassicGroupExecutionState::PartitionCountHandoff { .. }
            | ClassicGroupExecutionState::SyncHandoff(_) => {
                return Err(ClassicGroupExecutionError::HandoffIncomplete);
            }
            ClassicGroupExecutionState::JoinConfirmationPending { .. }
            | ClassicGroupExecutionState::SyncDriverOwned(_)
            | ClassicGroupExecutionState::SyncConfirmationPending(_) => {
                return Ok(ClassicGroupCloseProgress::DriverOwned);
            }
            ClassicGroupExecutionState::CloseFault { revoke_failure } => {
                return Err(map_revocation_kind(revoke_failure.kind));
            }
            ClassicGroupExecutionState::PartitionCountsPostCore { .. } => {
                return Err(ClassicGroupExecutionError::PartitionCountsPostCore);
            }
            ClassicGroupExecutionState::Idle
                if owner.machine().phase() == ClassicGroupPhase::Closed =>
            {
                return Ok(ClassicGroupCloseProgress::AlreadyClosed);
            }
            ClassicGroupExecutionState::Idle
            | ClassicGroupExecutionState::PreparedJoin(_)
            | ClassicGroupExecutionState::PreparedPartitionCounts(_)
            | ClassicGroupExecutionState::PreparedSync(_) => {}
        }
        let transition = owner
            .apply(ClassicGroupInput::Close)
            .map_err(|error| ClassicGroupExecutionError::Core(error.kind()))?;
        match transition.into_effects().next() {
            None => {}
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }) => match retire_and_revoke_classic_group_assignment(
                owner,
                catalog,
                processing_lease,
                fetch,
                assignment,
                classic_generation,
            ) {
                Ok(_retirement) => {}
                Err(failure) => {
                    let kind = failure.kind;
                    self.set_execution_state(ClassicGroupExecutionState::CloseFault {
                        revoke_failure: failure,
                    });
                    return Err(map_revocation_kind(kind));
                }
            },
            Some(_) => return Err(ClassicGroupExecutionError::UnexpectedCloseEffect),
        }
        self.set_execution_state(ClassicGroupExecutionState::Idle);
        Ok(ClassicGroupCloseProgress::Progress)
    }

    pub(super) fn retry_close_fault(
        &mut self,
        owner: &ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::CloseFault { revoke_failure } = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::CloseNotFaulted);
        };
        match retire_and_revoke_classic_group_assignment(
            owner,
            catalog,
            processing_lease,
            fetch,
            revoke_failure.assignment,
            revoke_failure.classic_generation,
        ) {
            Ok(_retirement) => Ok(()),
            Err(failure) => {
                let kind = failure.kind;
                self.set_execution_state(ClassicGroupExecutionState::CloseFault {
                    revoke_failure: failure,
                });
                Err(map_revocation_kind(kind))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupCloseProgress {
    AlreadyClosed,
    Progress,
    DriverOwned,
}
