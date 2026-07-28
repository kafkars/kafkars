//! Offset-transfer health consequences applied to the active lifecycle.

use super::super::TransactionOffsetCommitConsequence;
use super::{
    TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleMachine,
    TransactionLifecycleMachineError, TransactionLifecycleState, TransactionLifecycleTransition,
};

impl TransactionLifecycleMachine {
    pub(super) fn settle_offset_commit(
        &mut self,
        epoch: TransactionEpoch,
        consequence: TransactionOffsetCommitConsequence,
    ) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
        self.require_epoch(epoch)?;
        match consequence {
            TransactionOffsetCommitConsequence::AbortRequired => match self.state {
                TransactionLifecycleState::Active => {
                    self.state = TransactionLifecycleState::AbortRequired;
                    Ok(TransactionLifecycleTransition::one(
                        TransactionLifecycleEffect::AbortRequired {
                            owner_id: self.owner_id,
                            epoch,
                        },
                    ))
                }
                TransactionLifecycleState::AbortRequired
                | TransactionLifecycleState::DrainingAbort
                | TransactionLifecycleState::EndingAbort
                | TransactionLifecycleState::Fatal
                | TransactionLifecycleState::DrainingFatal => {
                    Ok(TransactionLifecycleTransition::none())
                }
                _ => Err(self.invalid_state()),
            },
            TransactionOffsetCommitConsequence::Fatal => match self.state {
                TransactionLifecycleState::Active
                | TransactionLifecycleState::AbortRequired
                | TransactionLifecycleState::DrainingAbort
                | TransactionLifecycleState::EndingAbort => Ok(self.enter_fatal()),
                TransactionLifecycleState::Fatal | TransactionLifecycleState::DrainingFatal => {
                    Ok(TransactionLifecycleTransition::none())
                }
                _ => Err(self.invalid_state()),
            },
        }
    }
}
