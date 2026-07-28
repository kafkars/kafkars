//! Enrollment, `RecordBatch` materialization, and initial Produce submission stages.

use kafka_client_core::{DeliveryStatus, Moment, TransactionSendIdentity, TransactionSendOutcome};

use crate::{
    driver::DriverOwner,
    producer::materialization::TransactionalMaterializationBatch,
    protocol::produce::{MaterializedProduce, materialize_transactional_produce_batch},
    transaction::{TransactionLifecycleHostError, TransactionLifecycleTurn},
};

use super::{
    super::{
        aggregate::TransactionSendAggregate,
        model::{TransactionSendFailure, TransactionSendFailureKind, TransactionSendTurn},
        owner::TransactionSendOwner,
        port::{TransactionSendProducePort, TransactionSendProduceRequest},
    },
    PendingTransactionSend, PreparedTransactionSend, TransactionSendSlot,
};

impl TransactionSendOwner {
    pub(super) fn drive_enrolling(
        &mut self,
        pending: PendingTransactionSend,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        if lifecycle.drive_enrollment(now, driver)? == TransactionLifecycleTurn::Progress {
            self.slot = TransactionSendSlot::Enrolling(pending);
            return Ok(TransactionSendTurn::Progress);
        }
        let Some(terminal) = lifecycle.take_enrollment_terminal() else {
            self.slot = TransactionSendSlot::Enrolling(pending);
            return Ok(TransactionSendTurn::Idle);
        };
        self.settle_enrollment(pending, terminal, lifecycle);
        Ok(TransactionSendTurn::Progress)
    }

    pub(super) fn materialize(
        &mut self,
        pending: PendingTransactionSend,
        batch: TransactionalMaterializationBatch,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        match materialize_transactional_produce_batch(batch, self.compression) {
            Ok(materialized) => {
                if pending.sticky {
                    self.partitioners
                        .partition_batch_sealed(pending.topic_id, pending.partition.partition());
                }
                self.slot = TransactionSendSlot::Materialized(pending, materialized);
            }
            Err(_error) => self.finish_unproduced(
                pending,
                TransactionSendOutcome::FailedHealthy,
                TransactionSendFailure::new(
                    TransactionSendFailureKind::Materialization,
                    DeliveryStatus::NotSent,
                ),
                lifecycle,
            )?,
        }
        Ok(TransactionSendTurn::Progress)
    }

    pub(super) fn submit(
        &mut self,
        pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        port: &mut dyn TransactionSendProducePort,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let mut pending = pending;
        if pending.prepared.is_none() {
            let transactional_id = match lifecycle.transactional_id_owner() {
                Ok(owner) => owner,
                Err(error) => {
                    self.slot = TransactionSendSlot::Materialized(pending, materialized);
                    return Err(error);
                }
            };
            let producer = match lifecycle.producer_identity() {
                Ok(producer) => producer,
                Err(error) => {
                    self.slot = TransactionSendSlot::Materialized(pending, materialized);
                    return Err(error);
                }
            };
            let identity = TransactionSendIdentity::new(
                producer,
                pending.partition,
                pending.sequence,
                pending.deadline.core(),
            );
            let attempt =
                match lifecycle.prepare_send_attempt(pending.epoch, pending.send_id, identity) {
                    Ok(attempt) => attempt,
                    Err(error) => {
                        self.slot = TransactionSendSlot::Materialized(pending, materialized);
                        return Err(error);
                    }
                };
            pending.prepared = Some(PreparedTransactionSend {
                transactional_id,
                identity,
                attempt,
            });
        }
        let prepared = pending
            .prepared
            .as_ref()
            .unwrap_or_else(|| unreachable!("driver submission retains prepared identity"));
        let request = TransactionSendProduceRequest {
            epoch: pending.epoch,
            send_id: pending.send_id,
            attempt: prepared.attempt,
            transactional_id: prepared.transactional_id.as_ref(),
            materialized: &materialized,
            now,
            deadline: pending.deadline,
        };
        match port.submit(request) {
            Ok(call) => self.slot = TransactionSendSlot::Producing(pending, materialized, call),
            Err(failure) => {
                drop(materialized);
                self.finish_unproduced(
                    pending,
                    TransactionSendOutcome::FailedHealthy,
                    TransactionSendFailure::new(
                        TransactionSendFailureKind::ProduceSubmission(failure.kind),
                        failure.delivery,
                    ),
                    lifecycle,
                )?;
            }
        }
        Ok(TransactionSendTurn::Progress)
    }
}
