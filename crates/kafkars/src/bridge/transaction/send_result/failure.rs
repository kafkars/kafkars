//! Transactional send failure translation and retained delivery certainty.

use kafka_client_engine::{
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailure,
    TransactionSendFailureKind,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

pub(super) fn translate_send_failure(failure: TransactionSendFailure) -> KafkaError {
    translate_send_failure_parts(
        failure.kind(),
        failure.delivery(),
        failure.broker_code(),
        failure.consequence(),
    )
}

pub(in crate::bridge::transaction) fn translate_send_failure_parts(
    kind: TransactionSendFailureKind,
    delivery: TransactionSendDeliveryStatus,
    broker_code: Option<i16>,
    consequence: TransactionSendConsequence,
) -> KafkaError {
    let public = translate_send_failure_kind_with_code(kind, broker_code);
    let error = KafkaError::new(public, format!("transactional send failed: {kind:?}"))
        .with_delivery_status(translate_send_delivery(delivery))
        .with_broker_code(broker_code);
    match consequence {
        TransactionSendConsequence::FailedHealthy => error,
        TransactionSendConsequence::AbortRequired => error.with_transaction_abort_required(),
        TransactionSendConsequence::Fatal => error.with_fatal_disposition(),
    }
}

const fn translate_send_failure_kind_with_code(
    kind: TransactionSendFailureKind,
    broker_code: Option<i16>,
) -> ErrorKind {
    match kind {
        TransactionSendFailureKind::Fenced if matches!(broker_code, Some(47 | 90)) => {
            ErrorKind::Fenced
        }
        TransactionSendFailureKind::Fenced => ErrorKind::Broker,
        _ => translate_send_failure_kind(kind),
    }
}

pub(in crate::bridge::transaction) const fn translate_send_failure_kind(
    kind: TransactionSendFailureKind,
) -> ErrorKind {
    match kind {
        TransactionSendFailureKind::Busy
        | TransactionSendFailureKind::Backpressure
        | TransactionSendFailureKind::DriverRejected => ErrorKind::Backpressure,
        TransactionSendFailureKind::StaleTransaction
        | TransactionSendFailureKind::OwnerUnavailable
        | TransactionSendFailureKind::ProducerIdentity => ErrorKind::State,
        TransactionSendFailureKind::InvalidTarget | TransactionSendFailureKind::InvalidRecord => {
            ErrorKind::InvalidRecord
        }
        TransactionSendFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        TransactionSendFailureKind::Transport
        | TransactionSendFailureKind::NameResolution
        | TransactionSendFailureKind::ConnectionUnavailable => ErrorKind::Transport,
        TransactionSendFailureKind::Compatibility => ErrorKind::Compatibility,
        TransactionSendFailureKind::Access => ErrorKind::Access,
        TransactionSendFailureKind::Coordinator | TransactionSendFailureKind::Routing => {
            ErrorKind::Routing
        }
        TransactionSendFailureKind::Fenced
        | TransactionSendFailureKind::InvalidResponse
        | TransactionSendFailureKind::Broker => ErrorKind::Broker,
        TransactionSendFailureKind::Identity => ErrorKind::Identity,
        TransactionSendFailureKind::DriverClosed
        | TransactionSendFailureKind::Materialization
        | TransactionSendFailureKind::Permanent
        | TransactionSendFailureKind::Correlation => ErrorKind::Internal,
    }
}

const fn translate_send_delivery(delivery: TransactionSendDeliveryStatus) -> DeliveryStatus {
    match delivery {
        TransactionSendDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        TransactionSendDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}
