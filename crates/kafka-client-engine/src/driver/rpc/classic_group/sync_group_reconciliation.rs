//! Exact post-driver-shutdown reconciliation of Sync call ownership.

use super::{
    sync_group_calls::AcceptedSyncGroupCall,
    sync_group_settlement::RecoveredSyncGroupConfirmation,
    sync_group_terminal::{
        RecoveredSyncGroupCall, SyncGroupCallKey, SyncGroupCompletionFailure, SyncGroupTerminal,
    },
};

enum RecoveredSyncGroupState {
    Active(RecoveredSyncGroupCall),
    Settled(SyncGroupTerminal),
    PendingConfirmation(RecoveredSyncGroupConfirmation),
    Completion(SyncGroupCompletionFailure),
}

/// One exact Sync call owner recovered after the embedded driver is gone.
#[must_use = "recovered SyncGroup ownership must reconcile with its accepted-call receipt"]
pub(crate) struct RecoveredSyncGroupOwnership {
    recovered_sync_group_state: RecoveredSyncGroupState,
}

impl RecoveredSyncGroupOwnership {
    pub(super) const fn seal_recovered_sync_group_active(active: RecoveredSyncGroupCall) -> Self {
        Self {
            recovered_sync_group_state: RecoveredSyncGroupState::Active(active),
        }
    }

    pub(super) const fn seal_recovered_sync_group_settled(settled: SyncGroupTerminal) -> Self {
        Self {
            recovered_sync_group_state: RecoveredSyncGroupState::Settled(settled),
        }
    }

    pub(super) const fn seal_recovered_sync_group_pending(
        pending: RecoveredSyncGroupConfirmation,
    ) -> Self {
        Self {
            recovered_sync_group_state: RecoveredSyncGroupState::PendingConfirmation(pending),
        }
    }

    pub(super) const fn seal_recovered_sync_group_completion(
        completion: SyncGroupCompletionFailure,
    ) -> Self {
        Self {
            recovered_sync_group_state: RecoveredSyncGroupState::Completion(completion),
        }
    }

    pub(crate) const fn key(&self) -> SyncGroupCallKey {
        match &self.recovered_sync_group_state {
            RecoveredSyncGroupState::Active(active) => active.key(),
            RecoveredSyncGroupState::Settled(settled) => settled.key(),
            RecoveredSyncGroupState::PendingConfirmation(pending) => pending.key(),
            RecoveredSyncGroupState::Completion(completion) => completion.key(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "a mismatch must return the exact generated Sync response without allocation"
    )]
    pub(crate) fn reconcile_sync_group_after_driver_shutdown(
        self,
        accepted: AcceptedSyncGroupCall,
    ) -> Result<(), SyncGroupShutdownReconciliationFailure> {
        let recovered = self.key();
        let supplied = accepted.key();
        if recovered != supplied {
            return Err(SyncGroupShutdownReconciliationFailure {
                accepted,
                recovered: self,
                error: SyncGroupShutdownReconciliationError::KeyMismatch {
                    recovered,
                    supplied,
                },
            });
        }
        self.consume_recovered_sync_group_ownership();
        accepted.consume_sync_group_shutdown_receipt();
        Ok(())
    }

    fn consume_recovered_sync_group_ownership(self) {
        match self.recovered_sync_group_state {
            RecoveredSyncGroupState::Active(active) => drop(active),
            RecoveredSyncGroupState::Settled(settled) => drop(settled),
            RecoveredSyncGroupState::PendingConfirmation(pending) => drop(pending),
            RecoveredSyncGroupState::Completion(completion) => drop(completion),
        }
    }

    #[cfg(test)]
    pub(crate) const fn active_for_test(key: SyncGroupCallKey) -> Self {
        Self::seal_recovered_sync_group_active(RecoveredSyncGroupCall::new(key))
    }
}

/// Why recovered Sync ownership could not consume an accepted-call receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncGroupShutdownReconciliationError {
    KeyMismatch {
        recovered: SyncGroupCallKey,
        supplied: SyncGroupCallKey,
    },
}

/// Failed Sync reconciliation retaining both exact linear owners unchanged.
#[must_use = "failed SyncGroup shutdown reconciliation still owns both receipts"]
pub(crate) struct SyncGroupShutdownReconciliationFailure {
    accepted: AcceptedSyncGroupCall,
    recovered: RecoveredSyncGroupOwnership,
    error: SyncGroupShutdownReconciliationError,
}

impl SyncGroupShutdownReconciliationFailure {
    pub(crate) fn into_parts(
        self,
    ) -> (
        AcceptedSyncGroupCall,
        RecoveredSyncGroupOwnership,
        SyncGroupShutdownReconciliationError,
    ) {
        (self.accepted, self.recovered, self.error)
    }
}
