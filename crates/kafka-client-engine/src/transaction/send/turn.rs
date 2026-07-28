//! Enrollment, materialization, Produce submission, and evidence polling.

use std::sync::Arc;

use kafka_client_core::{
    Moment, TopicId, TransactionEpoch, TransactionPartition, TransactionSendAttempt,
    TransactionSendId, TransactionSendIdentity, TransactionSequenceLease,
};

use crate::{
    completion::CompletionId,
    driver::{DriverOwner, ProducerTopicViewCall},
    producer::materialization::TransactionalMaterializationBatch,
    protocol::produce::MaterializedProduce,
    transaction::{
        TransactionLifecycleHost, TransactionLifecycleHostError, TransactionSendReplacement,
    },
};

use super::{
    aggregate::TransactionSendAggregate,
    input::TransactionSendRequest,
    model::TransactionSendTurn,
    owner::TransactionSendOwner,
    port::{
        DriverTransactionSendProducePort, TransactionSendProduceCall,
        TransactionSendProduceEvidence, TransactionSendProducePort,
    },
};

mod execution;
mod retry;

pub(super) struct PreparedTransactionSend {
    pub(super) transactional_id: Arc<str>,
    pub(super) identity: TransactionSendIdentity,
    pub(super) attempt: TransactionSendAttempt,
}

pub(super) struct PendingTransactionSend {
    pub(super) completion_id: CompletionId,
    pub(super) epoch: TransactionEpoch,
    pub(super) send_id: TransactionSendId,
    pub(super) partition: TransactionPartition,
    pub(super) sequence: TransactionSequenceLease,
    pub(super) deadline: crate::clock::OperationDeadline,
    pub(super) topic_id: TopicId,
    pub(super) sticky: bool,
    pub(super) prepared: Option<PreparedTransactionSend>,
}

pub(super) struct PendingTransactionPartitioning {
    pub(super) completion_id: CompletionId,
    pub(super) epoch: TransactionEpoch,
    pub(super) send_id: TransactionSendId,
    pub(super) request: TransactionSendRequest,
}

pub(super) enum TransactionSendSlot {
    Vacant,
    Reserved(TransactionSendRequest, CompletionId),
    AwaitingPartition(PendingTransactionPartitioning),
    Partitioning(PendingTransactionPartitioning, ProducerTopicViewCall),
    Enrolling(PendingTransactionSend),
    Ready(PendingTransactionSend, TransactionalMaterializationBatch),
    Materialized(PendingTransactionSend, MaterializedProduce),
    Producing(
        PendingTransactionSend,
        MaterializedProduce,
        Box<dyn TransactionSendProduceCall>,
    ),
    Settling(
        PendingTransactionSend,
        MaterializedProduce,
        Box<dyn TransactionSendProduceEvidence>,
    ),
    Invalidating(
        PendingTransactionSend,
        MaterializedProduce,
        Box<dyn TransactionSendProduceEvidence>,
        TransactionSendReplacement,
    ),
    RetryBackoff(
        PendingTransactionSend,
        MaterializedProduce,
        Box<dyn TransactionSendProduceEvidence>,
        TransactionSendReplacement,
    ),
    Terminal(CompletionId, super::model::TransactionSendTerminal),
    Published,
}

impl TransactionSendOwner {
    pub(crate) fn turn(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        self.turn_with(
            lifecycle,
            now,
            driver,
            &mut DriverTransactionSendProducePort::new(driver),
        )
    }

    pub(in crate::transaction) fn turn_with(
        &mut self,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
        driver: &DriverOwner,
        port: &mut dyn TransactionSendProducePort,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        if self.turn_completion()? {
            return Ok(TransactionSendTurn::Progress);
        }
        let slot = core::mem::replace(&mut self.slot, TransactionSendSlot::Vacant);
        match slot {
            TransactionSendSlot::Vacant
            | TransactionSendSlot::Terminal(_, _)
            | TransactionSendSlot::Published => {
                self.slot = slot;
                Ok(TransactionSendTurn::Idle)
            }
            TransactionSendSlot::Reserved(_, _) => {
                unreachable!("send reservation never crosses an admission call")
            }
            TransactionSendSlot::AwaitingPartition(pending) => {
                self.submit_partitioning(pending, lifecycle, now, driver)
            }
            TransactionSendSlot::Partitioning(pending, call) => {
                self.poll_partitioning(pending, call, lifecycle)
            }
            TransactionSendSlot::Enrolling(pending) => {
                self.drive_enrolling(pending, lifecycle, now, driver)
            }
            TransactionSendSlot::Ready(pending, batch) => {
                self.materialize(pending, batch, lifecycle)
            }
            TransactionSendSlot::Materialized(pending, materialized) => {
                self.submit(pending, materialized, lifecycle, now, port)
            }
            TransactionSendSlot::Producing(pending, materialized, mut call) => {
                let Some(evidence) = call.try_terminal() else {
                    self.slot = TransactionSendSlot::Producing(pending, materialized, call);
                    return Ok(TransactionSendTurn::Idle);
                };
                self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
                Ok(TransactionSendTurn::Progress)
            }
            TransactionSendSlot::Settling(pending, materialized, evidence) => {
                self.settle_or_retry_produce(pending, materialized, evidence, lifecycle, now)
            }
            TransactionSendSlot::Invalidating(pending, materialized, evidence, replacement) => self
                .drive_invalidation(
                    pending,
                    materialized,
                    evidence,
                    replacement,
                    lifecycle,
                    now,
                    driver,
                ),
            TransactionSendSlot::RetryBackoff(pending, materialized, evidence, replacement) => self
                .drive_retry_backoff(
                    pending,
                    materialized,
                    evidence,
                    replacement,
                    lifecycle,
                    now,
                    port,
                ),
        }
    }
}
