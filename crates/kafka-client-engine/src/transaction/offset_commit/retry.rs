//! Bounded two-stage replacement scheduling under the original deadline.

use kafka_client_core::{Moment, TransactionOffsetCommitEffect, TransactionOffsetCommitInput};

use super::{
    model::TransactionOffsetCommitHostError, owner::TransactionOffsetCommitOwner,
    turn::PendingTransactionOffsetCommit,
};

impl TransactionOffsetCommitOwner {
    pub(super) fn authorize_retry(
        &mut self,
        pending: &mut PendingTransactionOffsetCommit,
        stage: kafka_client_core::TransactionOffsetCommitStage,
        now: Moment,
    ) -> Result<bool, TransactionOffsetCommitHostError> {
        if pending.retries_started >= self.retry_policy.max_retries()
            || pending.request.deadline().core().is_elapsed_at(now)
        {
            return Ok(false);
        }
        let Some(not_before) = now.checked_deadline_after(self.retry_policy.backoff_ticks()) else {
            return Ok(false);
        };
        if not_before >= pending.request.deadline().core() {
            return Ok(false);
        }
        let Some(retries_started) = pending.retries_started.checked_add(1) else {
            return Ok(false);
        };
        let transition = self
            .machine
            .apply(TransactionOffsetCommitInput::RetryableFailed {
                epoch: pending.request.epoch(),
                operation_id: pending.operation_id,
                stage,
            })?;
        let expected_effect = match stage {
            kafka_client_core::TransactionOffsetCommitStage::AddOffsets => {
                TransactionOffsetCommitEffect::SubmitAddOffsets {
                    epoch: pending.request.epoch(),
                    operation_id: pending.operation_id,
                    deadline: pending.request.deadline().core(),
                    group_fence: pending.request.group().fence(),
                }
            }
            kafka_client_core::TransactionOffsetCommitStage::TxnOffsetCommit => {
                TransactionOffsetCommitEffect::SubmitTxnOffsetCommit {
                    epoch: pending.request.epoch(),
                    operation_id: pending.operation_id,
                    deadline: pending.request.deadline().core(),
                    group_fence: pending.request.group().fence(),
                }
            }
        };
        if transition.into_effect() != Some(expected_effect) {
            return Err(TransactionOffsetCommitHostError::UnexpectedEffect);
        }
        pending.retry_not_before = Some(not_before);
        pending.retries_started = retries_started;
        Ok(true)
    }
}
