//! Exhaustive facade translation of transactional record-send outcomes.

use kafka_client_engine::{
    ProducerSendCaptureError, ProducerSendCaptureErrorKind, TransactionSendAdmissionErrorKind,
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailure,
    TransactionSendFailureKind, TransactionSendMetadata, TransactionSendObserverError,
    TransactionSendOutcome,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError, RecordMetadata};

use super::result::translate_control_kind;

pub(super) fn translate_send_capture(error: ProducerSendCaptureError) -> KafkaError {
    let kind = match error.kind() {
        ProducerSendCaptureErrorKind::DeadlineUnrepresentable => {
            TransactionSendAdmissionErrorKind::InvalidDeadline
        }
        ProducerSendCaptureErrorKind::TimestampUnrepresentable => {
            TransactionSendAdmissionErrorKind::TimestampUnavailable
        }
    };
    translate_send_admission(kind)
}

pub(super) fn translate_send_admission(kind: TransactionSendAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        TransactionSendAdmissionErrorKind::InvalidDeadline => ErrorKind::Timeout,
        TransactionSendAdmissionErrorKind::EmptyTopic
        | TransactionSendAdmissionErrorKind::NegativeExplicitPartition
        | TransactionSendAdmissionErrorKind::RetainedSizeOverflow
        | TransactionSendAdmissionErrorKind::InvalidPartition => ErrorKind::InvalidRecord,
        TransactionSendAdmissionErrorKind::Contended
        | TransactionSendAdmissionErrorKind::RetainedRecordBytes { .. }
        | TransactionSendAdmissionErrorKind::RetainedTopicCapacity { .. }
        | TransactionSendAdmissionErrorKind::RetainedTopicBytes { .. }
        | TransactionSendAdmissionErrorKind::Allocation
        | TransactionSendAdmissionErrorKind::Busy => ErrorKind::Backpressure,
        TransactionSendAdmissionErrorKind::Closed
        | TransactionSendAdmissionErrorKind::StaleOwner => ErrorKind::State,
        TransactionSendAdmissionErrorKind::TimestampUnavailable
        | TransactionSendAdmissionErrorKind::RetainedTopicBytesOverflow
        | TransactionSendAdmissionErrorKind::TopicIdentityExhausted
        | TransactionSendAdmissionErrorKind::SendIdentityExhausted => ErrorKind::Internal,
        TransactionSendAdmissionErrorKind::Transaction(kind) => {
            return translate_control_kind(kind).with_delivery_status(DeliveryStatus::NotSent);
        }
    };
    KafkaError::new(public, format!("transactional send rejected: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_send_observation(
    result: Result<TransactionSendOutcome, TransactionSendObserverError>,
) -> Result<RecordMetadata, KafkaError> {
    match result {
        Ok(TransactionSendOutcome::Succeeded(metadata)) => Ok(translate_send_metadata(&metadata)),
        Ok(TransactionSendOutcome::Failed(failure)) => Err(translate_send_failure(failure)),
        Err(error) => Err(translate_send_observer_error(error)),
    }
}

fn translate_send_metadata(metadata: &TransactionSendMetadata) -> RecordMetadata {
    RecordMetadata::from_parts(
        metadata.topic().to_owned(),
        metadata.partition(),
        metadata.offset(),
        metadata.timestamp(),
        metadata.leader_epoch(),
    )
}

fn translate_send_failure(failure: TransactionSendFailure) -> KafkaError {
    translate_send_failure_parts(
        failure.kind(),
        failure.delivery(),
        failure.broker_code(),
        failure.consequence(),
    )
}

pub(super) fn translate_send_failure_parts(
    kind: TransactionSendFailureKind,
    delivery: TransactionSendDeliveryStatus,
    broker_code: Option<i16>,
    consequence: TransactionSendConsequence,
) -> KafkaError {
    let public = if consequence == TransactionSendConsequence::Fatal {
        ErrorKind::Fenced
    } else {
        translate_send_failure_kind(kind)
    };
    let error = KafkaError::new(public, format!("transactional send failed: {kind:?}"))
        .with_delivery_status(translate_send_delivery(delivery))
        .with_broker_code(broker_code);
    if consequence == TransactionSendConsequence::AbortRequired {
        error.with_transaction_abort_required()
    } else {
        error
    }
}

pub(super) const fn translate_send_failure_kind(kind: TransactionSendFailureKind) -> ErrorKind {
    match kind {
        TransactionSendFailureKind::Busy
        | TransactionSendFailureKind::Backpressure
        | TransactionSendFailureKind::DriverRejected => ErrorKind::Backpressure,
        TransactionSendFailureKind::StaleTransaction
        | TransactionSendFailureKind::OwnerUnavailable => ErrorKind::State,
        TransactionSendFailureKind::InvalidTarget => ErrorKind::InvalidRecord,
        TransactionSendFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        TransactionSendFailureKind::Transport
        | TransactionSendFailureKind::NameResolution
        | TransactionSendFailureKind::ConnectionUnavailable => ErrorKind::Transport,
        TransactionSendFailureKind::Compatibility => ErrorKind::Compatibility,
        TransactionSendFailureKind::InvalidResponse | TransactionSendFailureKind::Broker => {
            ErrorKind::Broker
        }
        TransactionSendFailureKind::Routing => ErrorKind::Routing,
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

fn translate_send_observer_error(error: TransactionSendObserverError) -> KafkaError {
    let public = match error {
        TransactionSendObserverError::AlreadyObserved | TransactionSendObserverError::Stale => {
            ErrorKind::State
        }
        TransactionSendObserverError::InternalInvariant => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
