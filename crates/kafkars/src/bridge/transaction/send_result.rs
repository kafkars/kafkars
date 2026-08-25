//! Exhaustive facade translation of transactional record-send outcomes.

mod failure;

use kafka_client_engine::{
    ProducerSendCaptureError, ProducerSendCaptureErrorKind, TransactionBatchSendOutcome,
    TransactionSendAdmissionErrorKind, TransactionSendMetadata, TransactionSendObserverError,
    TransactionSendOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, RecordMetadata, TopicUuid,
    transaction::TransactionBatchMetadata,
};

use super::result::translate_control_kind;
use failure::translate_send_failure;
#[cfg(test)]
pub(super) use failure::{translate_send_failure_kind, translate_send_failure_parts};

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
        TransactionSendAdmissionErrorKind::EmptyBatch
        | TransactionSendAdmissionErrorKind::EmptyTopic
        | TransactionSendAdmissionErrorKind::NegativeExplicitPartition
        | TransactionSendAdmissionErrorKind::MissingExplicitPartition
        | TransactionSendAdmissionErrorKind::MixedBatchTopic
        | TransactionSendAdmissionErrorKind::MixedBatchPartition
        | TransactionSendAdmissionErrorKind::RetainedSizeOverflow
        | TransactionSendAdmissionErrorKind::InvalidPartition => ErrorKind::InvalidRecord,
        TransactionSendAdmissionErrorKind::MixedBatchTopicIdentity => ErrorKind::Identity,
        TransactionSendAdmissionErrorKind::Contended
        | TransactionSendAdmissionErrorKind::BatchRecordCapacity { .. }
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
    let error = KafkaError::new(public, format!("transactional send rejected: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent);
    if kind == TransactionSendAdmissionErrorKind::Contended {
        error.with_safe_retry()
    } else {
        error
    }
}

pub(super) fn translate_send_batch_observation(
    result: Result<TransactionBatchSendOutcome, TransactionSendObserverError>,
) -> Result<TransactionBatchMetadata, KafkaError> {
    match result {
        Ok(TransactionBatchSendOutcome::Succeeded(metadata)) => {
            let topic_uuid = translate_topic_uuid(metadata.topic_uuid())?;
            Ok(TransactionBatchMetadata::from_parts(
                metadata.topic().to_owned(),
                topic_uuid,
                metadata.partition(),
                metadata.base_offset(),
                metadata.last_offset(),
                metadata.record_count(),
                metadata.timestamp(),
                metadata.leader_epoch(),
            ))
        }
        Ok(TransactionBatchSendOutcome::Failed(failure)) => Err(translate_send_failure(failure)),
        Err(error) => Err(translate_send_observer_error(error)),
    }
}

pub(super) fn translate_send_observation(
    result: Result<TransactionSendOutcome, TransactionSendObserverError>,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
) -> Result<RecordMetadata, KafkaError> {
    match result {
        Ok(TransactionSendOutcome::Succeeded(metadata)) => {
            translate_send_metadata(&metadata, serialized_key_size, serialized_value_size)
        }
        Ok(TransactionSendOutcome::Failed(failure)) => Err(translate_send_failure(failure)),
        Err(error) => Err(translate_send_observer_error(error)),
    }
}

fn translate_send_metadata(
    metadata: &TransactionSendMetadata,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
) -> Result<RecordMetadata, KafkaError> {
    let topic_uuid = translate_topic_uuid(metadata.topic_uuid())?;
    Ok(translate_send_metadata_parts(
        metadata.topic().to_owned(),
        topic_uuid,
        metadata.partition(),
        metadata.offset(),
        metadata.timestamp(),
        metadata.leader_epoch(),
        serialized_key_size,
        serialized_value_size,
    ))
}

pub(super) fn translate_topic_uuid(raw: Option<[u8; 16]>) -> Result<Option<TopicUuid>, KafkaError> {
    match raw {
        None => Ok(None),
        Some(bytes) => TopicUuid::try_from_bytes(bytes).map(Some).ok_or_else(|| {
            KafkaError::new(
                ErrorKind::Identity,
                "transactional success retained an invalid zero topic UUID",
            )
            .with_delivery_status(DeliveryStatus::PossiblySent)
            .with_transaction_abort_required()
            .with_fatal_disposition()
        }),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the lossless translation boundary names every transactional delivery fact"
)]
pub(super) fn translate_send_metadata_parts(
    topic: String,
    topic_uuid: Option<TopicUuid>,
    partition: i32,
    offset: i64,
    timestamp: Option<i64>,
    leader_epoch: Option<i32>,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
) -> RecordMetadata {
    RecordMetadata::from_parts_with_topic_uuid(
        topic,
        topic_uuid,
        partition,
        offset,
        timestamp,
        leader_epoch,
        serialized_key_size,
        serialized_value_size,
    )
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
