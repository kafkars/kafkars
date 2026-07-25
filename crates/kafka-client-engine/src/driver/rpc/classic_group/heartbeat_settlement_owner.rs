//! Exact Heartbeat terminal handoff, restoration, confirmation, and shutdown recovery.

use super::{
    heartbeat_calls::{
        AcceptedClassicHeartbeatCall, TrackedClassicHeartbeatCall, TrackedClassicHeartbeatCalls,
    },
    heartbeat_reconciliation::RecoveredClassicHeartbeatOwnership,
    heartbeat_settlement::{
        ClassicHeartbeatBeginError, ClassicHeartbeatConfirmationError,
        ClassicHeartbeatConfirmationFailure, ClassicHeartbeatRestoreError,
        ClassicHeartbeatRestoreFailure, PendingClassicHeartbeatConfirmation,
        RecoveredClassicHeartbeatConfirmation, SettledClassicHeartbeatCall,
    },
    heartbeat_terminal::{ClassicHeartbeatCompletionFailure, ClassicHeartbeatTerminal},
};

/// Complete post-driver recovery of every retained Heartbeat call state.
#[must_use = "shutdown recovery retains accepted Heartbeat ownership"]
pub(crate) struct ClassicHeartbeatShutdownRecovery {
    active: Vec<TrackedClassicHeartbeatCall>,
    settled: Option<ClassicHeartbeatTerminal>,
    pending: Option<RecoveredClassicHeartbeatConfirmation>,
    completion: Option<ClassicHeartbeatCompletionFailure>,
}

impl ClassicHeartbeatShutdownRecovery {
    #[cfg(test)]
    pub(crate) fn active_storage_capacity_for_test(&self) -> usize {
        self.active.capacity()
    }

    pub(crate) fn pop_active(&mut self) -> Option<RecoveredClassicHeartbeatOwnership> {
        self.active
            .pop()
            .map(TrackedClassicHeartbeatCall::recover_after_driver_shutdown)
            .map(RecoveredClassicHeartbeatOwnership::seal_active)
    }

    pub(crate) fn take_settled(&mut self) -> Option<RecoveredClassicHeartbeatOwnership> {
        self.settled
            .take()
            .map(RecoveredClassicHeartbeatOwnership::seal_settled)
    }

    pub(crate) fn take_pending(&mut self) -> Option<RecoveredClassicHeartbeatOwnership> {
        self.pending
            .take()
            .map(RecoveredClassicHeartbeatOwnership::seal_pending)
    }

    pub(crate) fn take_completion(&mut self) -> Option<RecoveredClassicHeartbeatOwnership> {
        self.completion
            .take()
            .map(RecoveredClassicHeartbeatOwnership::seal_completion)
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

impl TrackedClassicHeartbeatCalls {
    pub(crate) fn begin_classic_heartbeat_settlement(
        &mut self,
        accepted: &AcceptedClassicHeartbeatCall,
    ) -> Result<ClassicHeartbeatTerminal, ClassicHeartbeatBeginError> {
        let supplied = accepted.key();
        if let Some(pending) = &self.pending_confirmation {
            return Err(ClassicHeartbeatBeginError::ConfirmationPending {
                pending: pending.key(),
            });
        }
        let Some(settled) = self.settled.as_ref() else {
            return Err(ClassicHeartbeatBeginError::NoSettlement { supplied });
        };
        if settled.key() != supplied {
            return Err(ClassicHeartbeatBeginError::KeyMismatch {
                settled: settled.key(),
                supplied,
            });
        }
        let Some(settled) = self.settled.take() else {
            return Err(ClassicHeartbeatBeginError::NoSettlement { supplied });
        };
        let (terminal, pending) = settled.into_parts();
        self.pending_confirmation = Some(pending);
        Ok(terminal)
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed confirmation must return the exact linear Heartbeat receipt"
    )]
    pub(crate) fn confirm_classic_heartbeat_settlement(
        &mut self,
        accepted: AcceptedClassicHeartbeatCall,
    ) -> Result<(), ClassicHeartbeatConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::NoPending { supplied },
            ));
        };
        pending.confirm_classic_heartbeat_route_token();
        accepted.confirm_classic_heartbeat_call_receipt();
        Ok(())
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed restoration must return the exact generated Heartbeat response"
    )]
    pub(crate) fn restore_classic_heartbeat_settlement(
        &mut self,
        terminal: ClassicHeartbeatTerminal,
    ) -> Result<(), ClassicHeartbeatRestoreFailure> {
        let supplied = terminal.key();
        if self.settled.is_some() {
            return Err(ClassicHeartbeatRestoreFailure::new(
                terminal,
                ClassicHeartbeatRestoreError::SettlementPresent { supplied },
            ));
        }
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(ClassicHeartbeatRestoreFailure::new(
                terminal,
                ClassicHeartbeatRestoreError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(ClassicHeartbeatRestoreFailure::new(
                terminal,
                ClassicHeartbeatRestoreError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(ClassicHeartbeatRestoreFailure::new(
                terminal,
                ClassicHeartbeatRestoreError::NoPending { supplied },
            ));
        };
        self.settled = Some(pending.into_settled(terminal));
        Ok(())
    }

    pub(crate) fn recover_classic_heartbeats_after_driver_shutdown(
        self,
    ) -> ClassicHeartbeatShutdownRecovery {
        ClassicHeartbeatShutdownRecovery {
            active: self.calls,
            settled: self
                .settled
                .map(SettledClassicHeartbeatCall::recover_after_driver_shutdown),
            pending: self
                .pending_confirmation
                .map(PendingClassicHeartbeatConfirmation::recover_after_driver_shutdown),
            completion: self.completion_failure,
        }
    }
}
