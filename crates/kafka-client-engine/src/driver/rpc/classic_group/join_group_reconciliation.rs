//! Exact post-driver-shutdown reconciliation of Join call ownership.

use super::{
    join_group_calls::AcceptedJoinGroupCall,
    join_group_settlement::RecoveredJoinGroupConfirmation,
    join_group_terminal::{
        JoinGroupCallKey, JoinGroupCompletionFailure, JoinGroupTerminal, RecoveredJoinGroupCall,
    },
};

enum RecoveredJoinGroupState {
    Active(RecoveredJoinGroupCall),
    Settled(JoinGroupTerminal),
    PendingConfirmation(RecoveredJoinGroupConfirmation),
    Completion(JoinGroupCompletionFailure),
}

/// One exact Join call owner recovered after the embedded driver is gone.
#[must_use = "recovered JoinGroup ownership must reconcile with its accepted-call receipt"]
pub(crate) struct RecoveredJoinGroupOwnership {
    recovered_join_group_state: RecoveredJoinGroupState,
}

impl RecoveredJoinGroupOwnership {
    pub(super) const fn seal_recovered_join_group_active(active: RecoveredJoinGroupCall) -> Self {
        Self {
            recovered_join_group_state: RecoveredJoinGroupState::Active(active),
        }
    }

    pub(super) const fn seal_recovered_join_group_settled(settled: JoinGroupTerminal) -> Self {
        Self {
            recovered_join_group_state: RecoveredJoinGroupState::Settled(settled),
        }
    }

    pub(super) const fn seal_recovered_join_group_pending(
        pending: RecoveredJoinGroupConfirmation,
    ) -> Self {
        Self {
            recovered_join_group_state: RecoveredJoinGroupState::PendingConfirmation(pending),
        }
    }

    pub(super) const fn seal_recovered_join_group_completion(
        completion: JoinGroupCompletionFailure,
    ) -> Self {
        Self {
            recovered_join_group_state: RecoveredJoinGroupState::Completion(completion),
        }
    }

    pub(crate) const fn key(&self) -> JoinGroupCallKey {
        match &self.recovered_join_group_state {
            RecoveredJoinGroupState::Active(active) => active.key(),
            RecoveredJoinGroupState::Settled(settled) => settled.key(),
            RecoveredJoinGroupState::PendingConfirmation(pending) => pending.key(),
            RecoveredJoinGroupState::Completion(completion) => completion.key(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "a mismatch must return the exact generated Join response without allocation"
    )]
    pub(crate) fn reconcile_join_group_after_driver_shutdown(
        self,
        accepted: AcceptedJoinGroupCall,
    ) -> Result<(), JoinGroupShutdownReconciliationFailure> {
        let recovered = self.key();
        let supplied = accepted.key();
        if recovered != supplied {
            return Err(JoinGroupShutdownReconciliationFailure {
                accepted,
                recovered: self,
                error: JoinGroupShutdownReconciliationError::KeyMismatch {
                    recovered,
                    supplied,
                },
            });
        }
        self.consume_recovered_join_group_ownership();
        accepted.consume_join_group_shutdown_receipt();
        Ok(())
    }

    fn consume_recovered_join_group_ownership(self) {
        match self.recovered_join_group_state {
            RecoveredJoinGroupState::Active(active) => drop(active),
            RecoveredJoinGroupState::Settled(settled) => drop(settled),
            RecoveredJoinGroupState::PendingConfirmation(pending) => drop(pending),
            RecoveredJoinGroupState::Completion(completion) => drop(completion),
        }
    }

    #[cfg(test)]
    pub(super) const fn active_for_test(key: JoinGroupCallKey) -> Self {
        Self::seal_recovered_join_group_active(RecoveredJoinGroupCall::new(key))
    }
}

/// Why recovered Join ownership could not consume an accepted-call receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinGroupShutdownReconciliationError {
    KeyMismatch {
        recovered: JoinGroupCallKey,
        supplied: JoinGroupCallKey,
    },
}

/// Failed Join reconciliation retaining both exact linear owners unchanged.
#[must_use = "failed JoinGroup shutdown reconciliation still owns both receipts"]
pub(crate) struct JoinGroupShutdownReconciliationFailure {
    accepted: AcceptedJoinGroupCall,
    recovered: RecoveredJoinGroupOwnership,
    error: JoinGroupShutdownReconciliationError,
}

impl JoinGroupShutdownReconciliationFailure {
    pub(crate) fn into_parts(
        self,
    ) -> (
        AcceptedJoinGroupCall,
        RecoveredJoinGroupOwnership,
        JoinGroupShutdownReconciliationError,
    ) {
        (self.accepted, self.recovered, self.error)
    }
}
