//! Exact route invalidation, retry backoff, and replacement Produce submission.

use kafka_client_core::Moment;

use crate::{
    driver::{DriverOwner, transaction_produce::TransactionProduceRouteRefreshPoll},
    protocol::produce::MaterializedProduce,
    transaction::{TransactionLifecycleHostError, TransactionSendReplacement},
};

use super::{
    super::{
        aggregate::TransactionSendAggregate,
        model::TransactionSendTurn,
        owner::TransactionSendOwner,
        port::{
            TransactionSendProduceEvidence, TransactionSendProducePort,
            TransactionSendProduceRequest,
        },
    },
    PendingTransactionSend, TransactionSendSlot,
};

impl TransactionSendOwner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn drive_invalidation(
        &mut self,
        pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        mut evidence: Box<dyn TransactionSendProduceEvidence>,
        replacement: TransactionSendReplacement,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        if pending.deadline.core().is_elapsed_at(now) {
            return self.settle_retry_deadline(pending, materialized, evidence, lifecycle);
        }
        match evidence.poll_route_refresh(driver) {
            TransactionProduceRouteRefreshPoll::Submitted => {
                self.slot =
                    TransactionSendSlot::Invalidating(pending, materialized, evidence, replacement);
                Ok(TransactionSendTurn::Progress)
            }
            TransactionProduceRouteRefreshPoll::Pending => {
                self.slot =
                    TransactionSendSlot::Invalidating(pending, materialized, evidence, replacement);
                Ok(TransactionSendTurn::Idle)
            }
            TransactionProduceRouteRefreshPoll::Failed => {
                self.settle_produce(pending, materialized, evidence, lifecycle)
            }
            TransactionProduceRouteRefreshPoll::Ready => {
                self.slot =
                    TransactionSendSlot::RetryBackoff(pending, materialized, evidence, replacement);
                Ok(TransactionSendTurn::Progress)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn drive_retry_backoff(
        &mut self,
        mut pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        evidence: Box<dyn TransactionSendProduceEvidence>,
        replacement: TransactionSendReplacement,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        port: &mut dyn TransactionSendProducePort,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        if pending.deadline.core().is_elapsed_at(now) {
            return self.settle_retry_deadline(pending, materialized, evidence, lifecycle);
        }
        if !replacement.not_before.is_elapsed_at(now) {
            self.slot =
                TransactionSendSlot::RetryBackoff(pending, materialized, evidence, replacement);
            return Ok(TransactionSendTurn::Idle);
        }
        let Some(prepared) = pending.prepared.as_ref() else {
            self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        };
        if prepared.attempt != replacement.previous || prepared.identity != replacement.identity {
            self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        }
        let request = TransactionSendProduceRequest {
            epoch: pending.epoch,
            send_id: pending.send_id,
            attempt: replacement.replacement,
            transactional_id: prepared.transactional_id.as_ref(),
            materialized: &materialized,
            now,
            deadline: pending.deadline,
        };
        match port.submit(request) {
            Ok(call) => {
                evidence.discard();
                pending
                    .prepared
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("replacement remains prepared"))
                    .attempt = replacement.replacement;
                self.slot = TransactionSendSlot::Producing(pending, materialized, call);
                Ok(TransactionSendTurn::Progress)
            }
            Err(_failure) => self.settle_produce(pending, materialized, evidence, lifecycle),
        }
    }
}
