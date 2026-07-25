//! Exact post-driver-shutdown reconciliation of Heartbeat call ownership.

use super::{
    heartbeat_calls::AcceptedClassicHeartbeatCall,
    heartbeat_settlement::RecoveredClassicHeartbeatConfirmation,
    heartbeat_terminal::{
        ClassicHeartbeatCallKey, ClassicHeartbeatCompletionFailure, ClassicHeartbeatTerminal,
        RecoveredClassicHeartbeatCall,
    },
};

enum RecoveredClassicHeartbeatState {
    Active(RecoveredClassicHeartbeatCall),
    Settled(ClassicHeartbeatTerminal),
    PendingConfirmation(RecoveredClassicHeartbeatConfirmation),
    Completion(ClassicHeartbeatCompletionFailure),
}

/// One exact Heartbeat owner recovered after the embedded driver is gone.
#[must_use = "recovered Heartbeat ownership must reconcile with its accepted-call receipt"]
pub(crate) struct RecoveredClassicHeartbeatOwnership {
    state: RecoveredClassicHeartbeatState,
}

impl RecoveredClassicHeartbeatOwnership {
    pub(super) const fn seal_active(active: RecoveredClassicHeartbeatCall) -> Self {
        Self {
            state: RecoveredClassicHeartbeatState::Active(active),
        }
    }

    pub(super) const fn seal_settled(settled: ClassicHeartbeatTerminal) -> Self {
        Self {
            state: RecoveredClassicHeartbeatState::Settled(settled),
        }
    }

    pub(super) const fn seal_pending(pending: RecoveredClassicHeartbeatConfirmation) -> Self {
        Self {
            state: RecoveredClassicHeartbeatState::PendingConfirmation(pending),
        }
    }

    pub(super) const fn seal_completion(completion: ClassicHeartbeatCompletionFailure) -> Self {
        Self {
            state: RecoveredClassicHeartbeatState::Completion(completion),
        }
    }

    pub(crate) const fn key(&self) -> ClassicHeartbeatCallKey {
        match &self.state {
            RecoveredClassicHeartbeatState::Active(active) => active.key(),
            RecoveredClassicHeartbeatState::Settled(settled) => settled.key(),
            RecoveredClassicHeartbeatState::PendingConfirmation(pending) => pending.key(),
            RecoveredClassicHeartbeatState::Completion(completion) => completion.key(),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "a mismatch must return the exact generated Heartbeat response without allocation"
    )]
    pub(crate) fn reconcile_classic_heartbeat_after_driver_shutdown(
        self,
        accepted: AcceptedClassicHeartbeatCall,
    ) -> Result<(), ClassicHeartbeatShutdownReconciliationFailure> {
        let recovered = self.key();
        let supplied = accepted.key();
        if recovered != supplied {
            return Err(ClassicHeartbeatShutdownReconciliationFailure {
                accepted,
                recovered: self,
                error: ClassicHeartbeatShutdownReconciliationError::KeyMismatch {
                    recovered,
                    supplied,
                },
            });
        }
        self.consume();
        accepted.consume_classic_heartbeat_shutdown_receipt();
        Ok(())
    }

    fn consume(self) {
        match self.state {
            RecoveredClassicHeartbeatState::Active(active) => drop(active),
            RecoveredClassicHeartbeatState::Settled(settled) => drop(settled),
            RecoveredClassicHeartbeatState::PendingConfirmation(pending) => drop(pending),
            RecoveredClassicHeartbeatState::Completion(completion) => drop(completion),
        }
    }

    #[cfg(test)]
    pub(crate) const fn active_for_test(key: ClassicHeartbeatCallKey) -> Self {
        Self::seal_active(RecoveredClassicHeartbeatCall::new(key))
    }
}

/// Why recovered Heartbeat ownership could not consume an accepted receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatShutdownReconciliationError {
    KeyMismatch {
        recovered: ClassicHeartbeatCallKey,
        supplied: ClassicHeartbeatCallKey,
    },
}

/// Failed reconciliation retaining both exact linear owners unchanged.
#[must_use = "failed Heartbeat shutdown reconciliation still owns both receipts"]
pub(crate) struct ClassicHeartbeatShutdownReconciliationFailure {
    accepted: AcceptedClassicHeartbeatCall,
    recovered: RecoveredClassicHeartbeatOwnership,
    error: ClassicHeartbeatShutdownReconciliationError,
}

impl ClassicHeartbeatShutdownReconciliationFailure {
    pub(crate) fn into_parts(
        self,
    ) -> (
        AcceptedClassicHeartbeatCall,
        RecoveredClassicHeartbeatOwnership,
        ClassicHeartbeatShutdownReconciliationError,
    ) {
        (self.accepted, self.recovered, self.error)
    }
}
