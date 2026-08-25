//! Attempt-correlated Produce settlement and core-authorized replacement.

use kafka_client_core::{
    DeliveryStatus, Moment, ProducerBrokerFailureKind, TransactionSendAttemptFailure,
    TransactionSequenceSettlement,
};

use crate::{
    driver::transaction_produce::{TransactionProduceFailureKind, TransactionProduceTerminalFact},
    protocol::produce::MaterializedProduce,
    transaction::TransactionLifecycleHostError,
};

use super::{
    aggregate::TransactionSendAggregate,
    model::TransactionSendTurn,
    model::{TransactionSendFailure, TransactionSendFailureKind},
    owner::TransactionSendOwner,
    port::TransactionSendProduceEvidence,
    terminal::{failure_terminal, normalized_produce_terminal},
    turn::{PendingTransactionSend, TransactionSendSlot},
};

impl TransactionSendOwner {
    pub(super) fn settle_retry_deadline(
        &mut self,
        pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        evidence: Box<dyn TransactionSendProduceEvidence>,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let Some(delivery) = retry_failure_delivery(&pending, evidence.as_ref()) else {
            return self.settle_produce(pending, materialized, evidence, lifecycle);
        };
        let failure =
            TransactionSendFailure::new(TransactionSendFailureKind::DeadlineElapsed, delivery);
        match lifecycle.settle_accepted(
            pending.epoch,
            pending.send_id,
            pending.partition,
            pending.sequence,
            TransactionSequenceSettlement::NotAppended,
        ) {
            Ok(outcome) => {
                evidence.discard();
                drop(materialized);
                self.slot = TransactionSendSlot::Terminal(
                    pending.completion_id,
                    failure_terminal(&pending, outcome, failure),
                );
                Ok(TransactionSendTurn::Progress)
            }
            Err(error) => {
                self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
                Err(error)
            }
        }
    }

    pub(super) fn settle_produce(
        &mut self,
        pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        evidence: Box<dyn TransactionSendProduceEvidence>,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let (settlement, terminal) =
            normalized_produce_terminal(&pending, evidence.attempt(), evidence.fact());
        match lifecycle.settle_accepted(
            pending.epoch,
            pending.send_id,
            pending.partition,
            pending.sequence,
            settlement,
        ) {
            Ok(_) => {
                evidence.discard();
                drop(materialized);
                self.slot = TransactionSendSlot::Terminal(pending.completion_id, terminal);
                Ok(TransactionSendTurn::Progress)
            }
            Err(error) => {
                self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
                Err(error)
            }
        }
    }

    pub(super) fn settle_or_retry_produce(
        &mut self,
        pending: PendingTransactionSend,
        materialized: MaterializedProduce,
        evidence: Box<dyn TransactionSendProduceEvidence>,
        lifecycle: &mut dyn TransactionSendAggregate,
        now: Moment,
    ) -> Result<TransactionSendTurn, TransactionLifecycleHostError> {
        let Some(failure) = routing_failure(evidence.fact()) else {
            return self.settle_produce(pending, materialized, evidence, lifecycle);
        };
        if pending.expected_topic_uuid.is_some() {
            return self.settle_produce(pending, materialized, evidence, lifecycle);
        }
        let Some(prepared) = pending.prepared.as_ref() else {
            self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        };
        let attempt = evidence.attempt();
        if prepared.attempt != attempt {
            return self.settle_produce(pending, materialized, evidence, lifecycle);
        }
        let identity = prepared.identity;
        let replacement = match lifecycle.authorize_send_replacement(
            pending.epoch,
            pending.send_id,
            attempt,
            now,
            TransactionSendAttemptFailure::Broker(failure),
        ) {
            Ok(replacement) => replacement,
            Err(error) => {
                self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
                return Err(error);
            }
        };
        let Some(replacement) = replacement else {
            return self.settle_produce(pending, materialized, evidence, lifecycle);
        };
        if replacement.previous != attempt || replacement.identity != identity {
            self.slot = TransactionSendSlot::Settling(pending, materialized, evidence);
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        }
        self.slot = TransactionSendSlot::Invalidating(pending, materialized, evidence, replacement);
        Ok(TransactionSendTurn::Progress)
    }
}

fn retry_failure_delivery(
    pending: &PendingTransactionSend,
    evidence: &dyn TransactionSendProduceEvidence,
) -> Option<DeliveryStatus> {
    if pending.prepared.as_ref().map(|prepared| prepared.attempt) != Some(evidence.attempt()) {
        return None;
    }
    match evidence.fact() {
        TransactionProduceTerminalFact::AbortRequired {
            epoch,
            send_id,
            failure,
        } if epoch == pending.epoch && send_id == pending.send_id => Some(failure.delivery()),
        TransactionProduceTerminalFact::Succeeded { .. }
        | TransactionProduceTerminalFact::AbortRequired { .. }
        | TransactionProduceTerminalFact::Fatal { .. } => None,
    }
}

const fn routing_failure(
    fact: TransactionProduceTerminalFact,
) -> Option<kafka_client_core::ProducerBrokerFailure> {
    match fact {
        TransactionProduceTerminalFact::AbortRequired { failure, .. } => match failure.kind() {
            TransactionProduceFailureKind::Broker(failure)
                if matches!(failure.kind(), ProducerBrokerFailureKind::Routing) =>
            {
                Some(failure)
            }
            _ => None,
        },
        TransactionProduceTerminalFact::Succeeded { .. }
        | TransactionProduceTerminalFact::Fatal { .. } => None,
    }
}
