//! Exhaustive facade translation of transactional offset outcomes.

use kafka_client_engine::{
    TransactionOffsetsAdmissionErrorKind, TransactionOffsetsConsequence,
    TransactionOffsetsDeliveryStatus, TransactionOffsetsFailure, TransactionOffsetsFailureKind,
    TransactionOffsetsObserverError, TransactionOffsetsOutcome,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError};

pub(super) fn translate_admission(kind: TransactionOffsetsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        TransactionOffsetsAdmissionErrorKind::InvalidDeadline => ErrorKind::Timeout,
        TransactionOffsetsAdmissionErrorKind::StaleCheckpoint
        | TransactionOffsetsAdmissionErrorKind::StaleOwner
        | TransactionOffsetsAdmissionErrorKind::InvalidLifecycle
        | TransactionOffsetsAdmissionErrorKind::Closed => ErrorKind::State,
        TransactionOffsetsAdmissionErrorKind::Contended
        | TransactionOffsetsAdmissionErrorKind::Busy
        | TransactionOffsetsAdmissionErrorKind::Backpressure => ErrorKind::Backpressure,
        TransactionOffsetsAdmissionErrorKind::InvalidInput => ErrorKind::InvalidRecord,
        TransactionOffsetsAdmissionErrorKind::IdentityExhausted => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("transactional offsets rejected: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_observation(
    result: Result<TransactionOffsetsOutcome, TransactionOffsetsObserverError>,
) -> Result<(), KafkaError> {
    match result {
        Ok(TransactionOffsetsOutcome::Succeeded) => Ok(()),
        Ok(TransactionOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_failure(failure: TransactionOffsetsFailure) -> KafkaError {
    let kind = if failure.consequence() == TransactionOffsetsConsequence::Fatal {
        ErrorKind::Fenced
    } else {
        match failure.kind() {
            TransactionOffsetsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
            TransactionOffsetsFailureKind::DriverRejected
            | TransactionOffsetsFailureKind::Backpressure => ErrorKind::Backpressure,
            TransactionOffsetsFailureKind::Compatibility => ErrorKind::Compatibility,
            TransactionOffsetsFailureKind::Transport => ErrorKind::Transport,
            TransactionOffsetsFailureKind::Broker
            | TransactionOffsetsFailureKind::InvalidResponse => ErrorKind::Broker,
            TransactionOffsetsFailureKind::Correlation
            | TransactionOffsetsFailureKind::DriverClosed => ErrorKind::Internal,
        }
    };
    let error = KafkaError::new(
        kind,
        format!(
            "transactional offset {:?} failed: {:?}",
            failure.stage(),
            failure.kind()
        ),
    )
    .with_delivery_status(match failure.delivery() {
        TransactionOffsetsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        TransactionOffsetsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    })
    .with_broker_code(failure.broker_code());
    if failure.consequence() == TransactionOffsetsConsequence::AbortRequired {
        error.with_transaction_abort_required()
    } else {
        error
    }
}

fn translate_observer_error(error: TransactionOffsetsObserverError) -> KafkaError {
    let kind = match error {
        TransactionOffsetsObserverError::AlreadyObserved
        | TransactionOffsetsObserverError::Stale => ErrorKind::State,
        TransactionOffsetsObserverError::InternalInvariant => ErrorKind::Internal,
    };
    KafkaError::new(kind, error.to_string())
}
