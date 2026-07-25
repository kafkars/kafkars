//! Lossless Sync transfer through the execution owner's guarded state operations.

use crate::driver::classic_group::{AcceptedSyncGroupCall, SyncGroupCallKey};

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync::{
        ClassicGroupSyncAcceptanceFailure, ClassicGroupSyncDriverOwner, ClassicGroupSyncIdentity,
        PreparedClassicGroupSync,
    },
};

impl ClassicGroupExecution {
    pub(super) const fn prepared_sync(&self) -> Option<&PreparedClassicGroupSync> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::PreparedSync(prepared) => Some(prepared),
            _ => None,
        }
    }

    pub(super) fn begin_sync_handoff(
        &mut self,
    ) -> Result<PreparedClassicGroupSync, ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PreparedSync(prepared) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        self.set_execution_state(ClassicGroupExecutionState::SyncHandoff(prepared.identity()));
        Ok(prepared)
    }

    pub(super) fn confirm_sync_driver_owned(
        &mut self,
        identity: ClassicGroupSyncIdentity,
        accepted: AcceptedSyncGroupCall,
    ) -> Result<(), ClassicGroupSyncAcceptanceFailure> {
        let expected_key =
            SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        let state = self.borrow_execution_state();
        let matches = matches!(
            state,
            ClassicGroupExecutionState::SyncHandoff(expected) if *expected == identity
        ) && accepted.key() == expected_key;
        if !matches {
            return Err(ClassicGroupSyncAcceptanceFailure::new(identity, accepted));
        }
        self.set_execution_state(ClassicGroupExecutionState::SyncDriverOwned(
            ClassicGroupSyncDriverOwner::new(identity, accepted),
        ));
        Ok(())
    }

    pub(super) fn finish_sync_submission_failure(
        &mut self,
        identity: ClassicGroupSyncIdentity,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.borrow_execution_state();
        let exact = matches!(
            state,
            ClassicGroupExecutionState::SyncHandoff(expected) if *expected == identity
        );
        if !exact {
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        }
        self.set_execution_state(ClassicGroupExecutionState::Idle);
        Ok(())
    }
}
