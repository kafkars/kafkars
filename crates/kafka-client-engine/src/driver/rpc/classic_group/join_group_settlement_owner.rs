//! Exact Join terminal handoff, restoration, confirmation, and shutdown recovery.

use super::{
    join_group_calls::{AcceptedJoinGroupCall, TrackedJoinGroupCall, TrackedJoinGroupCalls},
    join_group_reconciliation::RecoveredJoinGroupOwnership,
    join_group_settlement::{
        JoinGroupBeginError, JoinGroupConfirmationError, JoinGroupConfirmationFailure,
        JoinGroupRestoreError, JoinGroupRestoreFailure, PendingJoinGroupConfirmation,
        RecoveredJoinGroupConfirmation, SettledJoinGroupCall,
    },
    join_group_terminal::{JoinGroupCompletionFailure, JoinGroupTerminal},
};

/// Complete post-driver recovery of every retained Join call state.
#[must_use = "shutdown recovery retains accepted JoinGroup ownership"]
pub(crate) struct JoinGroupShutdownRecovery {
    active: Vec<super::join_group_calls::TrackedJoinGroupCall>,
    settled: Option<JoinGroupTerminal>,
    pending: Option<RecoveredJoinGroupConfirmation>,
    completion: Option<JoinGroupCompletionFailure>,
}

impl JoinGroupShutdownRecovery {
    #[cfg(test)]
    pub(crate) fn active_storage_capacity_for_test(&self) -> usize {
        self.active.capacity()
    }

    pub(crate) fn pop_active(&mut self) -> Option<RecoveredJoinGroupOwnership> {
        self.active
            .pop()
            .map(TrackedJoinGroupCall::recover_after_driver_shutdown)
            .map(RecoveredJoinGroupOwnership::seal_recovered_join_group_active)
    }

    pub(crate) fn take_settled(&mut self) -> Option<RecoveredJoinGroupOwnership> {
        self.settled
            .take()
            .map(RecoveredJoinGroupOwnership::seal_recovered_join_group_settled)
    }

    pub(crate) fn take_pending(&mut self) -> Option<RecoveredJoinGroupOwnership> {
        self.pending
            .take()
            .map(RecoveredJoinGroupOwnership::seal_recovered_join_group_pending)
    }

    pub(crate) fn take_completion(&mut self) -> Option<RecoveredJoinGroupOwnership> {
        self.completion
            .take()
            .map(RecoveredJoinGroupOwnership::seal_recovered_join_group_completion)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.retained_count() == 0
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.active
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending.is_some()))
            .saturating_add(usize::from(self.completion.is_some()))
    }
}

impl TrackedJoinGroupCalls {
    pub(crate) fn begin_join_group_settlement(
        &mut self,
        accepted: &AcceptedJoinGroupCall,
    ) -> Result<JoinGroupTerminal, JoinGroupBeginError> {
        let supplied = accepted.key();
        if let Some(pending) = &self.pending_confirmation {
            return Err(JoinGroupBeginError::ConfirmationPending {
                pending: pending.key(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(JoinGroupBeginError::NoSettlement { supplied });
        };
        if settled.key() != supplied {
            return Err(JoinGroupBeginError::KeyMismatch {
                settled: settled.key(),
                supplied,
            });
        }
        let Some(settled) = self.settled.take() else {
            return Err(JoinGroupBeginError::NoSettlement { supplied });
        };
        let (terminal, pending) = settled.into_parts();
        self.pending_confirmation = Some(pending);
        Ok(terminal)
    }

    pub(crate) fn confirm_join_group_settlement(
        &mut self,
        accepted: AcceptedJoinGroupCall,
    ) -> Result<(), JoinGroupConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::NoPending { supplied },
            ));
        };
        pending.confirm_join_group_route_token();
        accepted.confirm_join_group_call_receipt();
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the exact generated Join response"
    )]
    pub(crate) fn restore_join_group_settlement(
        &mut self,
        terminal: JoinGroupTerminal,
    ) -> Result<(), JoinGroupRestoreFailure> {
        let supplied = terminal.key();
        if self.settled.is_some() {
            return Err(JoinGroupRestoreFailure::new(
                terminal,
                JoinGroupRestoreError::SettlementPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(JoinGroupRestoreFailure::new(
                terminal,
                JoinGroupRestoreError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(JoinGroupRestoreFailure::new(
                terminal,
                JoinGroupRestoreError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(JoinGroupRestoreFailure::new(
                terminal,
                JoinGroupRestoreError::NoPending { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(terminal));
        Ok(())
    }

    pub(crate) fn recover_join_groups_after_driver_shutdown(self) -> JoinGroupShutdownRecovery {
        JoinGroupShutdownRecovery {
            active: self.calls,
            settled: self
                .settled
                .map(SettledJoinGroupCall::recover_after_driver_shutdown),
            pending: self
                .pending_confirmation
                .map(PendingJoinGroupConfirmation::recover_after_driver_shutdown),
            completion: self.completion_failure,
        }
    }
}
