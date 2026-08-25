//! Abort draining, owner loss, and `EndTxn` settlement transitions.

use crate::OperationId;

use super::machine::PendingTransactionEnd;
use super::{
    TransactionEndMode, TransactionEndObservation, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleEffect, TransactionLifecycleMachine, TransactionLifecycleMachineError,
    TransactionLifecycleState, TransactionLifecycleTerminal, TransactionLifecycleTransition,
};

impl TransactionLifecycleMachine {
    pub(super) fn abort(
        &mut self,
        epoch: TransactionEpoch,
        operation_id: OperationId,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if !matches!(
            self.state,
            TransactionLifecycleState::Active | TransactionLifecycleState::AbortRequired
        ) {
            return Err(self.invalid_state());
        }
        self.pending_end = Some(PendingTransactionEnd {
            mode: TransactionEndMode::Abort,
            operation_id: Some(operation_id),
        });
        Ok(self.begin_abort())
    }

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
            TransactionLifecycleState::Active | TransactionLifecycleState::AbortRequired => {
                self.owner_lost = true;
                self.pending_end = Some(PendingTransactionEnd {
                    mode: TransactionEndMode::Abort,
                    operation_id: None,
                });
                Ok(self.begin_abort())
            }
            TransactionLifecycleState::Fatal => {
                self.owner_lost = true;
                if self.outstanding_sends.is_empty() {
                    self.pending_end = None;
                    self.active_epoch = None;
                    self.state = TransactionLifecycleState::Closed;
                    Ok(TransactionLifecycleTransition::one(
                        TransactionLifecycleEffect::ReleaseOwner {
                            owner_id: self.owner_id,
                        },
                    ))
                } else {
                    self.state = TransactionLifecycleState::DrainingFatal;
                    Ok(TransactionLifecycleTransition::one(
                        TransactionLifecycleEffect::CancelFatalOutstanding {
                            owner_id: self.owner_id,
                            epoch: self.current_epoch(),
                            outstanding_sends: self.outstanding_sends.len(),
                        },
                    ))
                }
            }
            TransactionLifecycleState::DrainingFatal
            | TransactionLifecycleState::DrainingAbort
            | TransactionLifecycleState::EndingCommit
            | TransactionLifecycleState::EndingAbort
            | TransactionLifecycleState::Closed => Err(self.invalid_state()),
        }
    }

    fn begin_abort(&mut self) -> TransactionLifecycleTransition {
        let epoch = self.current_epoch();
        let observation = self.pending_observation();
        if self.outstanding_sends.is_empty() {
            self.submit_pending_end()
        } else {
            self.state = TransactionLifecycleState::DrainingAbort;
            TransactionLifecycleTransition::one(TransactionLifecycleEffect::CancelOutstanding {
                owner_id: self.owner_id,
                epoch,
                outstanding_sends: self.outstanding_sends.len(),
                observation,
            })
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
        if let TransactionEndOutcome::Failed(failure) = outcome {
            let pending = self
                .pending_end
                .as_ref()
                .unwrap_or_else(|| unreachable!("ending state retains end intent"));
            if failure.mode() != pending.mode {
                return Err(TransactionLifecycleMachineError::EndModeMismatch {
                    expected: pending.mode,
                    supplied: failure.mode(),
                });
            }
            return Ok(
                self.enter_fatal_with_terminal(Some(TransactionLifecycleTerminal::Failed(failure)))
            );
        }
        let pending = self
            .pending_end
            .take()
            .unwrap_or_else(|| unreachable!("ending state retains end intent"));
        let terminal = match pending.mode {
            TransactionEndMode::Commit => TransactionLifecycleTerminal::Committed,
            TransactionEndMode::Abort => TransactionLifecycleTerminal::Aborted,
        };
        self.outstanding_sends.clear();
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
            debug_assert_eq!(pending.mode, TransactionEndMode::Abort);
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
