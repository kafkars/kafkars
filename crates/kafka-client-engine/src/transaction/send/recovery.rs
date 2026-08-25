//! Exact transactional send reconciliation after driver destruction.

use kafka_client_core::{DeliveryStatus, TransactionSendOutcome};

use crate::transaction::{TransactionLifecycleHost, TransactionLifecycleHostError};

use super::{
    aggregate::TransactionSendAggregate,
    model::{TransactionSendFailureKind, TransactionSendTurn},
    owner::TransactionSendOwner,
    terminal::failure,
    turn::TransactionSendSlot,
};

impl TransactionSendOwner {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        self.recover_with(lifecycle)
    }

    pub(super) fn recover_with(
        &mut self,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let slot = core::mem::replace(&mut self.slot, TransactionSendSlot::Vacant);
        match slot {
            TransactionSendSlot::AwaitingPartition(pending) => {
                self.finish_unsequenced(
                    pending,
                    failure(
                        TransactionSendFailureKind::DriverShutdown,
                        DeliveryStatus::NotSent,
                    ),
                    TransactionSendOutcome::FailedHealthy,
                    lifecycle,
                )?;
            }
            TransactionSendSlot::Partitioning(pending, call) => {
                call.discard_after_driver_shutdown();
                self.finish_unsequenced(
                    pending,
                    failure(
                        TransactionSendFailureKind::DriverShutdown,
                        DeliveryStatus::NotSent,
                    ),
                    TransactionSendOutcome::FailedHealthy,
                    lifecycle,
                )?;
            }
            TransactionSendSlot::Enrolling(pending) => {
                lifecycle.recover_after_driver_shutdown()?;
                let terminal = lifecycle
                    .take_enrollment_terminal()
                    .unwrap_or_else(|| unreachable!("recovered enrollment retains a terminal"));
                self.settle_enrollment(pending, terminal, lifecycle);
            }
            TransactionSendSlot::Ready(pending, batch) => {
                drop(batch);
                self.finish_local_shutdown(pending, lifecycle)?;
            }
            TransactionSendSlot::Materialized(pending, materialized) => {
                drop(materialized);
                self.finish_local_shutdown(pending, lifecycle)?;
            }
            TransactionSendSlot::Producing(pending, materialized, call) => {
                self.settle_produce(
                    pending,
                    materialized,
                    call.recover_after_driver_shutdown(),
                    lifecycle,
                )?;
            }
            TransactionSendSlot::Settling(pending, materialized, evidence)
            | TransactionSendSlot::Invalidating(pending, materialized, evidence, _)
            | TransactionSendSlot::RetryBackoff(pending, materialized, evidence, _) => {
                self.settle_produce(pending, materialized, evidence, lifecycle)?;
            }
            TransactionSendSlot::Vacant
            | TransactionSendSlot::Terminal(_, _)
            | TransactionSendSlot::Published => self.slot = slot,
            TransactionSendSlot::Reserved(_, _) => {
                unreachable!("send reservation never crosses admission")
            }
        }
        Ok(TransactionSendTurn::Progress)
    }

    fn finish_local_shutdown(
        &mut self,
        pending: super::turn::PendingTransactionSend,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.finish_unproduced(
            pending,
            TransactionSendOutcome::FailedHealthy,
            failure(
                TransactionSendFailureKind::DriverShutdown,
                DeliveryStatus::NotSent,
            ),
            lifecycle,
        )
    }
}
