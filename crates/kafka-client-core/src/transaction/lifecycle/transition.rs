//! Atomic lifecycle dispatch, begin, commit, and fatal transitions.

use crate::{OperationId, TransactionalOwnerId};

use super::machine::PendingTransactionEnd;
use super::{
    TransactionEndMode, TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleInput,
    TransactionLifecycleMachine, TransactionLifecycleMachineError, TransactionLifecycleState,
    TransactionLifecycleTransition,
};

impl TransactionLifecycleMachine {
    /// Applies one normalized fact without hidden I/O, retry, or protocol policy.
    pub fn apply(
        &mut self,
        owner_id: TransactionalOwnerId,
        input: TransactionLifecycleInput,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        if owner_id != self.owner_id {
            return Err(TransactionLifecycleMachineError::OwnerMismatch {
                expected: self.owner_id,
                supplied: owner_id,
            });
        }
        match input {
            TransactionLifecycleInput::Begin => self.begin(),
            TransactionLifecycleInput::SendAccepted { epoch, send_id } => {
                self.accept_send(epoch, send_id)
            }
            TransactionLifecycleInput::SendPrepared {
                epoch,
                send_id,
                identity,
            } => self.prepare_send(epoch, send_id, identity),
            TransactionLifecycleInput::SendAttemptFailed {
                epoch,
                send_id,
                attempt,
                now,
                failure,
            } => self.fail_send_attempt(epoch, send_id, attempt, now, failure),
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id,
                outcome,
            } => self.settle_send(epoch, send_id, outcome),
            TransactionLifecycleInput::OffsetCommitSettled { epoch, consequence } => {
                self.settle_offset_commit(epoch, consequence)
            }
            TransactionLifecycleInput::Commit {
                epoch,
                operation_id,
            } => self.commit(epoch, operation_id),
            TransactionLifecycleInput::Abort {
                epoch,
                operation_id,
            } => self.abort(epoch, operation_id),
            TransactionLifecycleInput::EndRetryableBrokerRejected { epoch } => {
                self.retry_end(epoch)
            }
            TransactionLifecycleInput::EndSettled { epoch, outcome } => {
                self.settle_end(epoch, outcome)
            }
            TransactionLifecycleInput::OwnerLost => self.owner_lost(),
        }
    }

    fn begin(
        &mut self,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_state(TransactionLifecycleState::Idle)?;
        let epoch = self
            .next_epoch
            .ok_or(TransactionLifecycleMachineError::EpochExhausted)?;
        self.next_epoch = epoch.next();
        self.active_epoch = Some(epoch);
        self.state = TransactionLifecycleState::Active;
        Ok(TransactionLifecycleTransition::one(
            TransactionLifecycleEffect::Began {
                owner_id: self.owner_id,
                epoch,
            },
        ))
    }

    fn commit(
        &mut self,
        epoch: TransactionEpoch,
        operation_id: OperationId,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        if self.state == TransactionLifecycleState::AbortRequired {
            return Err(TransactionLifecycleMachineError::AbortRequired);
        }
        self.require_state(TransactionLifecycleState::Active)?;
        if !self.outstanding_sends.is_empty() {
            return Err(TransactionLifecycleMachineError::OutstandingSends {
                count: self.outstanding_sends.len(),
            });
        }
        self.pending_end = Some(PendingTransactionEnd {
            mode: TransactionEndMode::Commit,
            operation_id: Some(operation_id),
        });
        Ok(self.submit_pending_end())
    }

    pub(super) fn enter_fatal(&mut self) -> TransactionLifecycleTransition {
        self.enter_fatal_with_terminal(None)
    }

    pub(super) fn enter_fatal_with_terminal(
        &mut self,
        end_terminal: Option<super::TransactionLifecycleTerminal>,
    ) -> TransactionLifecycleTransition {
        let operation_id = self.pending_end.take().and_then(|end| end.operation_id);
        let terminal = operation_id
            .map(|_| end_terminal.unwrap_or(super::TransactionLifecycleTerminal::Fatal));
        self.state = TransactionLifecycleState::Fatal;
        TransactionLifecycleTransition::one(TransactionLifecycleEffect::EnterFatal {
            owner_id: self.owner_id,
            epoch: self.current_epoch(),
            operation_id,
            terminal,
            owner_lost: self.owner_lost,
        })
    }

    pub(super) fn current_epoch(&self) -> TransactionEpoch {
        self.active_epoch
            .unwrap_or_else(|| unreachable!("active lifecycle stage retains one epoch"))
    }

    pub(super) fn require_epoch(
        &self,
        supplied: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleMachineError> {
        let Some(expected) = self.active_epoch else {
            return Err(self.invalid_state());
        };
        if expected != supplied {
            return Err(TransactionLifecycleMachineError::EpochMismatch { expected, supplied });
        }
        Ok(())
    }

    pub(super) fn require_state(
        &self,
        expected: TransactionLifecycleState,
    ) -> Result<(), TransactionLifecycleMachineError> {
        if self.state == expected {
            Ok(())
        } else {
            Err(self.invalid_state())
        }
    }

    pub(super) const fn invalid_state(&self) -> TransactionLifecycleMachineError {
        TransactionLifecycleMachineError::InvalidState { state: self.state }
    }
}
