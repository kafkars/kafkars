//! Exact Sync terminal handoff, restoration, confirmation, and shutdown recovery.

use super::{
    sync_group_calls::{AcceptedSyncGroupCall, TrackedSyncGroupCall, TrackedSyncGroupCalls},
    sync_group_settlement::{
        PendingSyncGroupConfirmation, RecoveredSyncGroupConfirmation, SettledSyncGroupCall,
        SyncGroupBeginError, SyncGroupConfirmationError, SyncGroupConfirmationFailure,
        SyncGroupRestoreError, SyncGroupRestoreFailure,
    },
    sync_group_terminal::{RecoveredSyncGroupCall, SyncGroupCompletionFailure, SyncGroupTerminal},
};

/// Complete post-driver recovery of every retained Sync call state.
#[must_use = "shutdown recovery retains accepted SyncGroup ownership"]
pub(crate) struct SyncGroupShutdownRecovery {
    active: Vec<super::sync_group_calls::TrackedSyncGroupCall>,
    settled: Option<SyncGroupTerminal>,
    pending: Option<RecoveredSyncGroupConfirmation>,
    completion: Option<SyncGroupCompletionFailure>,
}

impl SyncGroupShutdownRecovery {
    #[cfg(test)]
    pub(crate) fn active_storage_capacity_for_test(&self) -> usize {
        self.active.capacity()
    }

    pub(crate) fn pop_active(&mut self) -> Option<RecoveredSyncGroupCall> {
        self.active
            .pop()
            .map(TrackedSyncGroupCall::recover_after_driver_shutdown)
    }

    pub(crate) fn take_settled(&mut self) -> Option<SyncGroupTerminal> {
        self.settled.take()
    }

    pub(crate) fn take_pending(&mut self) -> Option<RecoveredSyncGroupConfirmation> {
        self.pending.take()
    }

    pub(crate) fn take_completion(&mut self) -> Option<SyncGroupCompletionFailure> {
        self.completion.take()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
            && self.settled.is_none()
            && self.pending.is_none()
            && self.completion.is_none()
    }
}

impl TrackedSyncGroupCalls {
    pub(crate) fn begin_sync_group_settlement(
        &mut self,
        accepted: &AcceptedSyncGroupCall,
    ) -> Result<SyncGroupTerminal, SyncGroupBeginError> {
        let supplied = accepted.key();
        if let Some(pending) = &self.pending_confirmation {
            return Err(SyncGroupBeginError::ConfirmationPending {
                pending: pending.key(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(SyncGroupBeginError::NoSettlement { supplied });
        };
        if settled.key() != supplied {
            return Err(SyncGroupBeginError::KeyMismatch {
                settled: settled.key(),
                supplied,
            });
        }
        let Some(settled) = self.settled.take() else {
            return Err(SyncGroupBeginError::NoSettlement { supplied });
        };
        let (terminal, pending) = settled.into_parts();
        self.pending_confirmation = Some(pending);
        Ok(terminal)
    }

    pub(crate) fn confirm_sync_group_settlement(
        &mut self,
        accepted: AcceptedSyncGroupCall,
    ) -> Result<(), SyncGroupConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::NoPending { supplied },
            ));
        };
        pending.confirm_sync_group_route_token();
        accepted.confirm_sync_group_call_receipt();
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the exact generated Sync response"
    )]
    pub(crate) fn restore_sync_group_settlement(
        &mut self,
        terminal: SyncGroupTerminal,
    ) -> Result<(), SyncGroupRestoreFailure> {
        let supplied = terminal.key();
        if self.settled.is_some() {
            return Err(SyncGroupRestoreFailure::new(
                terminal,
                SyncGroupRestoreError::SettlementPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(SyncGroupRestoreFailure::new(
                terminal,
                SyncGroupRestoreError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(SyncGroupRestoreFailure::new(
                terminal,
                SyncGroupRestoreError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(SyncGroupRestoreFailure::new(
                terminal,
                SyncGroupRestoreError::NoPending { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(terminal));
        Ok(())
    }

    pub(crate) fn recover_sync_groups_after_driver_shutdown(self) -> SyncGroupShutdownRecovery {
        SyncGroupShutdownRecovery {
            active: self.calls,
            settled: self
                .settled
                .map(SettledSyncGroupCall::recover_after_driver_shutdown),
            pending: self
                .pending_confirmation
                .map(PendingSyncGroupConfirmation::recover_after_driver_shutdown),
            completion: self.completion_failure,
        }
    }
}
