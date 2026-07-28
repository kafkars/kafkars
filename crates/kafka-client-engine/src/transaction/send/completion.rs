//! Bounded off-reactor terminal publication and fixed-slot reclamation.

use crate::{
    completion::{CompletionRegistryError, ReclaimStatus},
    transaction::TransactionLifecycleHostError,
};

use super::{
    input::{
        TransactionSendAdmissionFailure, TransactionSendAdmissionFailureKind,
        TransactionSendRequest,
    },
    owner::TransactionSendOwner,
    turn::TransactionSendSlot,
};

impl TransactionSendOwner {
    #[expect(
        clippy::result_large_err,
        reason = "reclamation failure returns the exact caller-owned send request"
    )]
    pub(super) fn reclaim_for_admission(
        &mut self,
        request: TransactionSendRequest,
    ) -> Result<TransactionSendRequest, TransactionSendAdmissionFailure> {
        if !matches!(self.slot, TransactionSendSlot::Published) {
            return Ok(request);
        }
        match self.reclaim_one() {
            Ok(_) => Ok(request),
            Err(error) => Err(TransactionSendAdmissionFailure::new(
                TransactionSendAdmissionFailureKind::Lifecycle(error),
                request,
            )),
        }
    }

    pub(super) fn turn_completion(&mut self) -> Result<bool, TransactionLifecycleHostError> {
        if self.reclaim_one()? {
            return Ok(true);
        }
        self.publish_terminal()
    }

    pub(in crate::transaction) fn publish_terminal_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionLifecycleHostError> {
        if self.publish_terminal()? || !matches!(self.slot, TransactionSendSlot::Terminal(_, _)) {
            return Ok(());
        }
        Err(CompletionRegistryError::NotificationBackpressure.into())
    }

    pub(crate) fn is_releasable_after_owner_close(&self) -> bool {
        matches!(
            self.slot,
            TransactionSendSlot::Vacant | TransactionSendSlot::Published
        )
    }

    fn publish_terminal(&mut self) -> Result<bool, TransactionLifecycleHostError> {
        let slot = core::mem::replace(&mut self.slot, TransactionSendSlot::Vacant);
        let TransactionSendSlot::Terminal(completion_id, terminal) = slot else {
            self.slot = slot;
            return Ok(false);
        };
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                self.slot = TransactionSendSlot::Published;
                Ok(true)
            }
            Err((CompletionRegistryError::NotificationBackpressure, terminal)) => {
                self.slot = TransactionSendSlot::Terminal(completion_id, terminal);
                Ok(false)
            }
            Err((error, terminal)) => {
                self.slot = TransactionSendSlot::Terminal(completion_id, terminal);
                Err(error.into())
            }
        }
    }

    fn reclaim_one(&mut self) -> Result<bool, TransactionLifecycleHostError> {
        if !matches!(self.slot, TransactionSendSlot::Published) {
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
                self.slot = TransactionSendSlot::Vacant;
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    }
}
