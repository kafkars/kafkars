//! Bounded off-reactor publication and fixed-slot reclamation.

pub(super) use crate::transaction::completion::TransactionOffsetCommitPublisher;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{
    model::TransactionOffsetCommitHostError, owner::TransactionOffsetCommitOwner,
    turn::TransactionOffsetCommitSlot,
};

impl TransactionOffsetCommitOwner {
    pub(super) fn turn_completion(&mut self) -> Result<bool, TransactionOffsetCommitHostError> {
        if self.reclaim_one()? {
            return Ok(true);
        }
        self.publish_terminal()
    }

    pub(in crate::transaction) fn publish_terminal_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionOffsetCommitHostError> {
        if self.publish_terminal()?
            || !matches!(self.slot, TransactionOffsetCommitSlot::Terminal(_, _))
        {
            return Ok(());
        }
        Err(CompletionRegistryError::NotificationBackpressure.into())
    }

    fn publish_terminal(&mut self) -> Result<bool, TransactionOffsetCommitHostError> {
        let slot = core::mem::replace(&mut self.slot, TransactionOffsetCommitSlot::Vacant);
        let TransactionOffsetCommitSlot::Terminal(completion_id, terminal) = slot else {
            self.slot = slot;
            return Ok(false);
        };
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                self.slot = TransactionOffsetCommitSlot::Published;
                Ok(true)
            }
            Err((CompletionRegistryError::NotificationBackpressure, terminal)) => {
                self.slot = TransactionOffsetCommitSlot::Terminal(completion_id, terminal);
                Ok(false)
            }
            Err((error, terminal)) => {
                self.slot = TransactionOffsetCommitSlot::Terminal(completion_id, terminal);
                Err(error.into())
            }
        }
    }

    fn reclaim_one(&mut self) -> Result<bool, TransactionOffsetCommitHostError> {
        if !matches!(self.slot, TransactionOffsetCommitSlot::Published) {
            return Ok(false);
        }
        let completion_id = if let Some(completion_id) = self.reclaim_pending {
            completion_id
        } else {
            let Some(completion_id) = self.completions.next_reclaim()? else {
                return Ok(false);
            };
            self.reclaim_pending = Some(completion_id);
            completion_id
        };
        match self.completions.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(false),
            Ok(ReclaimStatus::Reclaimed) | Err(CompletionRegistryError::GenerationExhausted) => {
                self.reclaim_pending = None;
                self.slot = TransactionOffsetCommitSlot::Vacant;
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    }
}
