//! Closed Produce-to-sequence and lifecycle terminal normalization.

use kafka_client_core::{
    DeliveryStatus, TransactionSendAttempt, TransactionSendOutcome, TransactionSequenceSettlement,
};

use crate::driver::transaction_produce::{
    TransactionProduceFailure, TransactionProduceTerminalFact,
};

use super::{
    model::{TransactionSendFailure, TransactionSendFailureKind, TransactionSendTerminal},
    turn::PendingTransactionSend,
};

pub(super) fn normalized_produce_terminal(
    pending: &PendingTransactionSend,
    attempt: TransactionSendAttempt,
    fact: TransactionProduceTerminalFact,
) -> (TransactionSequenceSettlement, TransactionSendTerminal) {
    if pending.prepared.as_ref().map(|prepared| prepared.attempt) != Some(attempt) {
        return correlation_failure(pending);
    }
    match fact {
        TransactionProduceTerminalFact::Succeeded {
            epoch,
            send_id,
            success,
        } if epoch == pending.epoch && send_id == pending.send_id => {
            successful_terminal(pending, epoch, send_id, success)
        }
        TransactionProduceTerminalFact::AbortRequired {
            epoch,
            send_id,
            failure,
        } if epoch == pending.epoch && send_id == pending.send_id => (
            TransactionSequenceSettlement::NotAppended,
            TransactionSendTerminal::AbortRequired {
                epoch,
                send_id,
                failure: failure_from_produce(failure),
            },
        ),
        TransactionProduceTerminalFact::Fatal {
            epoch,
            send_id,
            failure,
        } if epoch == pending.epoch && send_id == pending.send_id => (
            TransactionSequenceSettlement::Uncertain,
            TransactionSendTerminal::Fatal {
                epoch,
                send_id,
                failure: failure_from_produce(failure),
            },
        ),
        _ => correlation_failure(pending),
    }
}

fn successful_terminal(
    pending: &PendingTransactionSend,
    epoch: kafka_client_core::TransactionEpoch,
    send_id: kafka_client_core::TransactionSendId,
    success: kafka_client_core::ProducerBatchSuccess,
) -> (TransactionSequenceSettlement, TransactionSendTerminal) {
    let Some(offset_delta) = pending
        .sequence
        .record_count()
        .checked_sub(1)
        .map(i64::from)
    else {
        return invalid_success_response(pending);
    };
    let Some(last_offset) = success.base_offset().checked_add(offset_delta) else {
        return invalid_success_response(pending);
    };
    (
        TransactionSequenceSettlement::Succeeded,
        TransactionSendTerminal::Succeeded {
            epoch,
            send_id,
            partition: pending.partition.partition(),
            success,
            last_offset,
        },
    )
}

const fn invalid_success_response(
    pending: &PendingTransactionSend,
) -> (TransactionSequenceSettlement, TransactionSendTerminal) {
    (
        TransactionSequenceSettlement::Uncertain,
        TransactionSendTerminal::Fatal {
            epoch: pending.epoch,
            send_id: pending.send_id,
            failure: failure(
                TransactionSendFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        },
    )
}

const fn correlation_failure(
    pending: &PendingTransactionSend,
) -> (TransactionSequenceSettlement, TransactionSendTerminal) {
    (
        TransactionSequenceSettlement::Uncertain,
        TransactionSendTerminal::Fatal {
            epoch: pending.epoch,
            send_id: pending.send_id,
            failure: failure(
                TransactionSendFailureKind::Correlation,
                DeliveryStatus::PossiblySent,
            ),
        },
    )
}

const fn failure_from_produce(source: TransactionProduceFailure) -> TransactionSendFailure {
    TransactionSendFailure::new(
        TransactionSendFailureKind::Produce(source.kind()),
        source.delivery(),
    )
}

pub(super) fn failure_terminal(
    pending: &PendingTransactionSend,
    outcome: TransactionSendOutcome,
    failure: TransactionSendFailure,
) -> TransactionSendTerminal {
    match outcome {
        TransactionSendOutcome::FailedHealthy => TransactionSendTerminal::FailedHealthy {
            epoch: pending.epoch,
            send_id: pending.send_id,
            failure,
        },
        TransactionSendOutcome::AbortRequired => TransactionSendTerminal::AbortRequired {
            epoch: pending.epoch,
            send_id: pending.send_id,
            failure,
        },
        TransactionSendOutcome::Fatal => TransactionSendTerminal::Fatal {
            epoch: pending.epoch,
            send_id: pending.send_id,
            failure,
        },
        TransactionSendOutcome::Succeeded => {
            unreachable!("unproduced send cannot succeed")
        }
    }
}

pub(super) const fn failure(
    kind: TransactionSendFailureKind,
    delivery: DeliveryStatus,
) -> TransactionSendFailure {
    TransactionSendFailure::new(kind, delivery)
}
