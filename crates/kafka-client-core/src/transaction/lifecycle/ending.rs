//! Owner loss, retry, and `EndTxn` settlement transitions.

use super::machine::PendingTransactionEnd;
use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleEffect, TransactionLifecycleMachine, TransactionLifecycleMachineError,
    TransactionLifecycleState, TransactionLifecycleTerminal, TransactionLifecycleTransition,
};

impl TransactionLifecycleMachine {
    pub(super) fn owner_lost(
        &mut self,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        match self.state {
            TransactionLifecycleState::Idle => {
                self.owner_lost = true;
                self.state = TransactionLifecycleState::Closed;
                Ok(TransactionLifecycleTransition::one(
                    TransactionLifecycleEffect::ReleaseOwner {
                        owner_id: self.owner_id,
                    },
                ))
            }
            TransactionLifecycleState::Active => {
                self.owner_lost = true;
                self.pending_end = Some(PendingTransactionEnd {
                    mode: TransactionEndMode::Abort,
                    operation_id: None,
                });
                Ok(self.submit_pending_end())
            }
            TransactionLifecycleState::Fatal => {
                self.owner_lost = true;
                self.pending_end = None;
                self.active_epoch = None;
                self.state = TransactionLifecycleState::Closed;
                Ok(TransactionLifecycleTransition::one(
                    TransactionLifecycleEffect::ReleaseOwner {
                        owner_id: self.owner_id,
                    },
                ))
            }
            TransactionLifecycleState::EndingCommit
            | TransactionLifecycleState::EndingAbort
            | TransactionLifecycleState::Closed => Err(self.invalid_state()),
        }
    }

    pub(super) fn submit_pending_end(&mut self) -> TransactionLifecycleTransition {
        let pending = self
            .pending_end
            .unwrap_or_else(|| unreachable!("end submission requires retained intent"));
        self.state = match pending.mode {
            TransactionEndMode::Commit => TransactionLifecycleState::EndingCommit,
            TransactionEndMode::Abort => TransactionLifecycleState::EndingAbort,
        };
        TransactionLifecycleTransition::one(TransactionLifecycleEffect::EndTransaction {
            owner_id: self.owner_id,
            epoch: self.current_epoch(),
            mode: pending.mode,
            observation: self.pending_observation(),
            operation_id: pending.operation_id,
        })
    }

    pub(super) fn retry_end(
        &mut self,
        epoch: TransactionEpoch,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if !matches!(
            self.state,
            TransactionLifecycleState::EndingCommit | TransactionLifecycleState::EndingAbort
        ) {
            return Err(self.invalid_state());
        }
        Ok(self.submit_pending_end())
    }

    pub(super) fn settle_end(
        &mut self,
        epoch: TransactionEpoch,
        outcome: TransactionEndOutcome,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if !matches!(
            self.state,
            TransactionLifecycleState::EndingCommit | TransactionLifecycleState::EndingAbort
        ) {
            return Err(self.invalid_state());
        }
        if outcome == TransactionEndOutcome::Fatal {
            return Ok(self.enter_fatal());
        }
        let pending = self
            .pending_end
            .take()
            .unwrap_or_else(|| unreachable!("ending state retains end intent"));
        let terminal = match pending.mode {
            TransactionEndMode::Commit => TransactionLifecycleTerminal::Committed,
            TransactionEndMode::Abort => TransactionLifecycleTerminal::Aborted,
        };
        self.active_epoch = None;
        if let Some(operation_id) = pending.operation_id {
            self.state = TransactionLifecycleState::Idle;
            Ok(TransactionLifecycleTransition::one(
                TransactionLifecycleEffect::Complete {
                    owner_id: self.owner_id,
                    epoch,
                    operation_id,
                    terminal,
                },
            ))
        } else {
            self.state = TransactionLifecycleState::Closed;
            Ok(TransactionLifecycleTransition::one(
                TransactionLifecycleEffect::ReleaseOwner {
                    owner_id: self.owner_id,
                },
            ))
        }
    }

    fn pending_observation(&self) -> TransactionEndObservation {
        match self.pending_end.and_then(|end| end.operation_id) {
            Some(_) => TransactionEndObservation::Observed,
            None => TransactionEndObservation::BestEffort,
        }
    }
}
