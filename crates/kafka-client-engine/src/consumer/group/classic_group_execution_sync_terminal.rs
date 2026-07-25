//! Guarded Sync terminal and route-confirmation state transitions.

use crate::driver::classic_group::TrackedSyncGroupCalls;

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync::ClassicGroupSyncDriverOwner,
};

impl ClassicGroupExecution {
    pub(super) const fn sync_driver_owner(&self) -> Option<&ClassicGroupSyncDriverOwner> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::SyncDriverOwned(owner)
            | ClassicGroupExecutionState::SyncConfirmationPending(owner) => Some(owner),
            _ => None,
        }
    }

    pub(super) fn stage_sync_confirmation(&mut self) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::SyncDriverOwned(owner) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        self.set_execution_state(ClassicGroupExecutionState::SyncConfirmationPending(owner));
        Ok(())
    }

    pub(super) fn confirm_sync(
        &mut self,
        calls: &mut TrackedSyncGroupCalls,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::SyncConfirmationPending(owner) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        let (identity, accepted) = owner.into_parts();
        match calls.confirm_sync_group_settlement(accepted) {
            Ok(()) => Ok(()),
            Err(failure) => {
                let (accepted, _error) = failure.into_parts();
                self.set_execution_state(ClassicGroupExecutionState::SyncConfirmationPending(
                    ClassicGroupSyncDriverOwner::new(identity, accepted),
                ));
                Err(ClassicGroupExecutionError::CallIdentityMismatch)
            }
        }
    }
}
