//! Atomic admission, stage advancement, rollback, and terminal assignment.

use crate::{Deadline, GroupPositionFence, TransactionEpoch};

use super::machine::PendingTransactionOffsetCommit;
use super::{
    TransactionOffsetCommitEffect, TransactionOffsetCommitId, TransactionOffsetCommitInput,
    TransactionOffsetCommitMachine, TransactionOffsetCommitMachineError,
    TransactionOffsetCommitStage, TransactionOffsetCommitState, TransactionOffsetCommitTerminal,
    TransactionOffsetCommitTransition,
};

impl TransactionOffsetCommitMachine {
    /// Admits one capacity-reserved transfer and consumes its nonreused identity.
    pub fn admit(
        &mut self,
        epoch: TransactionEpoch,
        deadline: Deadline,
        group_fence: GroupPositionFence,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        if self.state != TransactionOffsetCommitState::Idle {
            return Err(self.invalid_state());
        }
        let operation_id = self
            .next_operation_id
            .ok_or(TransactionOffsetCommitMachineError::IdentityExhausted)?;
        self.next_operation_id = operation_id.checked_next();
        let pending = PendingTransactionOffsetCommit {
            epoch,
            operation_id,
            deadline,
            group_fence,
        };
        self.pending = Some(pending);
        self.state = TransactionOffsetCommitState::AddOffsetsAdmitted;
        Ok(TransactionOffsetCommitTransition::one(submit_add_offsets(
            pending,
        )))
    }

    /// Applies one exact normalized driver or broker fact.
    pub fn apply(
        &mut self,
        input: TransactionOffsetCommitInput,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        let (epoch, operation_id, stage) = input.correlation();
        let pending = self.require_correlation(epoch, operation_id)?;
        self.require_stage(stage)?;
        match input {
            TransactionOffsetCommitInput::DriverAccepted { .. } => self.driver_accepted(stage),
            TransactionOffsetCommitInput::DriverRejected { .. } => {
                self.driver_rejected(pending, stage)
            }
            TransactionOffsetCommitInput::Succeeded { .. } => self.succeeded(pending, stage),
            TransactionOffsetCommitInput::RetryableFailed { .. } => {
                self.retryable_failed(pending, stage)
            }
            TransactionOffsetCommitInput::AcceptedFailed { consequence, .. } => {
                self.accepted_failed(pending, stage, consequence)
            }
        }
    }

    fn driver_accepted(
        &mut self,
        stage: TransactionOffsetCommitStage,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        let expected = admitted_state(stage);
        if self.state != expected {
            return Err(self.invalid_state());
        }
        self.state = awaiting_state(stage);
        Ok(TransactionOffsetCommitTransition::none())
    }

    fn driver_rejected(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        if self.state != admitted_state(stage) {
            return Err(self.invalid_state());
        }
        Ok(self.finish(
            pending,
            TransactionOffsetCommitTerminal::RejectedNotSent { stage },
        ))
    }

    fn succeeded(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        if self.state != awaiting_state(stage) {
            return Err(self.invalid_state());
        }
        match stage {
            TransactionOffsetCommitStage::AddOffsets => {
                self.state = TransactionOffsetCommitState::TxnOffsetCommitAdmitted;
                Ok(TransactionOffsetCommitTransition::one(
                    submit_txn_offset_commit(pending),
                ))
            }
            TransactionOffsetCommitStage::TxnOffsetCommit => {
                Ok(self.finish(pending, TransactionOffsetCommitTerminal::Succeeded))
            }
        }
    }

    fn accepted_failed(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
        consequence: super::TransactionOffsetCommitConsequence,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        if self.state != awaiting_state(stage) {
            return Err(self.invalid_state());
        }
        Ok(self.finish(
            pending,
            TransactionOffsetCommitTerminal::Failed { stage, consequence },
        ))
    }

    fn retryable_failed(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        stage: TransactionOffsetCommitStage,
    ) -> Result<TransactionOffsetCommitTransition, TransactionOffsetCommitMachineError> {
        if self.state != awaiting_state(stage) {
            return Err(self.invalid_state());
        }
        self.state = admitted_state(stage);
        Ok(TransactionOffsetCommitTransition::one(match stage {
            TransactionOffsetCommitStage::AddOffsets => submit_add_offsets(pending),
            TransactionOffsetCommitStage::TxnOffsetCommit => submit_txn_offset_commit(pending),
        }))
    }

    fn finish(
        &mut self,
        pending: PendingTransactionOffsetCommit,
        terminal: TransactionOffsetCommitTerminal,
    ) -> TransactionOffsetCommitTransition {
        self.pending = None;
        self.state = TransactionOffsetCommitState::Idle;
        TransactionOffsetCommitTransition::one(TransactionOffsetCommitEffect::Complete {
            epoch: pending.epoch,
            operation_id: pending.operation_id,
            deadline: pending.deadline,
            group_fence: pending.group_fence,
            terminal,
        })
    }

    fn require_correlation(
        &self,
        epoch: TransactionEpoch,
        operation_id: TransactionOffsetCommitId,
    ) -> Result<PendingTransactionOffsetCommit, TransactionOffsetCommitMachineError> {
        let pending = self
            .pending
            .ok_or(TransactionOffsetCommitMachineError::NoOperation)?;
        Self::require_epoch(pending, epoch)?;
        if pending.operation_id != operation_id {
            return Err(TransactionOffsetCommitMachineError::OperationMismatch {
                expected: pending.operation_id,
                supplied: operation_id,
            });
        }
        Ok(pending)
    }

    fn require_stage(
        &self,
        supplied: TransactionOffsetCommitStage,
    ) -> Result<(), TransactionOffsetCommitMachineError> {
        let expected = match self.state {
            TransactionOffsetCommitState::AddOffsetsAdmitted
            | TransactionOffsetCommitState::AddOffsetsAwaiting => {
                TransactionOffsetCommitStage::AddOffsets
            }
            TransactionOffsetCommitState::TxnOffsetCommitAdmitted
            | TransactionOffsetCommitState::TxnOffsetCommitAwaiting => {
                TransactionOffsetCommitStage::TxnOffsetCommit
            }
            TransactionOffsetCommitState::Idle => {
                return Err(TransactionOffsetCommitMachineError::NoOperation);
            }
        };
        if supplied == expected {
            Ok(())
        } else {
            Err(TransactionOffsetCommitMachineError::StageMismatch { expected, supplied })
        }
    }

    const fn invalid_state(&self) -> TransactionOffsetCommitMachineError {
        TransactionOffsetCommitMachineError::InvalidState { state: self.state }
    }
}

const fn admitted_state(stage: TransactionOffsetCommitStage) -> TransactionOffsetCommitState {
    match stage {
        TransactionOffsetCommitStage::AddOffsets => {
            TransactionOffsetCommitState::AddOffsetsAdmitted
        }
        TransactionOffsetCommitStage::TxnOffsetCommit => {
            TransactionOffsetCommitState::TxnOffsetCommitAdmitted
        }
    }
}

const fn awaiting_state(stage: TransactionOffsetCommitStage) -> TransactionOffsetCommitState {
    match stage {
        TransactionOffsetCommitStage::AddOffsets => {
            TransactionOffsetCommitState::AddOffsetsAwaiting
        }
        TransactionOffsetCommitStage::TxnOffsetCommit => {
            TransactionOffsetCommitState::TxnOffsetCommitAwaiting
        }
    }
}

const fn submit_add_offsets(
    pending: PendingTransactionOffsetCommit,
) -> TransactionOffsetCommitEffect {
    TransactionOffsetCommitEffect::SubmitAddOffsets {
        epoch: pending.epoch,
        operation_id: pending.operation_id,
        deadline: pending.deadline,
        group_fence: pending.group_fence,
    }
}

const fn submit_txn_offset_commit(
    pending: PendingTransactionOffsetCommit,
) -> TransactionOffsetCommitEffect {
    TransactionOffsetCommitEffect::SubmitTxnOffsetCommit {
        epoch: pending.epoch,
        operation_id: pending.operation_id,
        deadline: pending.deadline,
        group_fence: pending.group_fence,
    }
}
