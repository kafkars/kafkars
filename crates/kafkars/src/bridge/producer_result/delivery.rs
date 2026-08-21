//! Lossless translation of terminal producer delivery and observer values.

use std::sync::Arc;

use kafka_client_engine::{
    ProducerDeliveryError as EngineDeliveryError, ProducerDeliveryFailure as EngineDeliveryFailure,
    ProducerDeliveryFailureKind as EngineFailureKind,
    ProducerDeliveryResult as EngineDeliveryResult, ProducerDeliveryStatus as EngineDeliveryStatus,
    ProducerObserverError as EngineObserverError, ProducerRecordMetadata as EngineRecordMetadata,
};

use crate::{DeliveryStatus, ErrorKind, KafkaError, RecordMetadata};

pub(crate) fn translate_delivery_result(
    topic: Arc<str>,
    create_timestamp: i64,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
    result: EngineDeliveryResult,
) -> Result<RecordMetadata, KafkaError> {
    match result {
        Ok(metadata) => translate_metadata(
            topic,
            create_timestamp,
            serialized_key_size,
            serialized_value_size,
            metadata,
        ),
        Err(error) => Err(translate_delivery_error(error)),
    }
}

pub(crate) fn translate_delivery_error(error: EngineDeliveryError) -> KafkaError {
    match error {
        EngineDeliveryError::Failed(failure) => translate_failure(failure),
        EngineDeliveryError::Observer(observer) => translate_observer_error(observer),
    }
}

fn translate_failure(failure: EngineDeliveryFailure) -> KafkaError {
    failure_error(
        failure.kind(),
        failure.delivery_status(),
        failure.broker_code(),
    )
}

pub(super) fn failure_error(
    kind: EngineFailureKind,
    status: EngineDeliveryStatus,
    broker_code: Option<i16>,
) -> KafkaError {
    let error = KafkaError::new(failure_kind(kind), failure_message(kind))
        .with_delivery_status(delivery_status(status))
        .with_broker_code(broker_code);
    terminal_disposition(error, kind, status)
}

fn terminal_disposition(
    error: KafkaError,
    kind: EngineFailureKind,
    status: EngineDeliveryStatus,
) -> KafkaError {
    match kind {
        EngineFailureKind::DriverRejected => match status {
            EngineDeliveryStatus::NotSent => error.with_safe_retry(),
            EngineDeliveryStatus::PossiblySent => error,
        },
        EngineFailureKind::Routing | EngineFailureKind::BrokerRetriable => match status {
            EngineDeliveryStatus::NotSent => error.with_safe_retry(),
            EngineDeliveryStatus::PossiblySent => error.with_duplicate_risk(),
        },
        EngineFailureKind::ProducerFenced
        | EngineFailureKind::ProducerIdentity
        | EngineFailureKind::InvalidResponse
        | EngineFailureKind::ExecutionUnavailable => error.with_fatal_disposition(),
        EngineFailureKind::Cancelled
        | EngineFailureKind::MaterializationFailed
        | EngineFailureKind::AccessRejected
        | EngineFailureKind::InvalidRecord
        | EngineFailureKind::Compatibility
        | EngineFailureKind::Transport
        | EngineFailureKind::DeadlineElapsed
        | EngineFailureKind::UnknownBroker => error,
    }
}

pub(super) const fn failure_kind(kind: EngineFailureKind) -> ErrorKind {
    match kind {
        EngineFailureKind::Cancelled => ErrorKind::Cancelled,
        EngineFailureKind::DriverRejected => ErrorKind::Backpressure,
        EngineFailureKind::MaterializationFailed | EngineFailureKind::ExecutionUnavailable => {
            ErrorKind::Internal
        }
        EngineFailureKind::Routing => ErrorKind::Routing,
        EngineFailureKind::BrokerRetriable
        | EngineFailureKind::InvalidResponse
        | EngineFailureKind::UnknownBroker => ErrorKind::Broker,
        EngineFailureKind::AccessRejected => ErrorKind::Access,
        EngineFailureKind::InvalidRecord => ErrorKind::InvalidRecord,
        EngineFailureKind::Compatibility => ErrorKind::Compatibility,
        EngineFailureKind::ProducerFenced => ErrorKind::Fenced,
        EngineFailureKind::ProducerIdentity => ErrorKind::State,
        EngineFailureKind::Transport => ErrorKind::Transport,
        EngineFailureKind::DeadlineElapsed => ErrorKind::Timeout,
    }
}

const fn failure_message(kind: EngineFailureKind) -> &'static str {
    match kind {
        EngineFailureKind::Cancelled => {
            "producer delivery was cancelled before transport ownership"
        }
        EngineFailureKind::DriverRejected => "driver rejected producer delivery",
        EngineFailureKind::MaterializationFailed => "producer record batch materialization failed",
        EngineFailureKind::Routing => "producer route is no longer valid",
        EngineFailureKind::BrokerRetriable => "Kafka returned a retryable producer failure",
        EngineFailureKind::AccessRejected => "Kafka rejected producer access",
        EngineFailureKind::InvalidRecord => "Kafka rejected producer record content",
        EngineFailureKind::Compatibility => "producer request is incompatible with Kafka",
        EngineFailureKind::ProducerFenced => "Kafka fenced the producer identity",
        EngineFailureKind::ProducerIdentity => "producer identity requires recovery",
        EngineFailureKind::Transport => "producer transport failed",
        EngineFailureKind::InvalidResponse => "Kafka returned an invalid producer response",
        EngineFailureKind::ExecutionUnavailable => "producer execution owner is unavailable",
        EngineFailureKind::DeadlineElapsed => "producer delivery deadline elapsed",
        EngineFailureKind::UnknownBroker => "Kafka returned an unknown producer failure",
    }
}

pub(super) const fn delivery_status(status: EngineDeliveryStatus) -> DeliveryStatus {
    match status {
        EngineDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        EngineDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: EngineObserverError) -> KafkaError {
    let kind = match error {
        EngineObserverError::AlreadyObserved | EngineObserverError::Stale => ErrorKind::State,
        EngineObserverError::TerminalTypeMismatch => ErrorKind::Internal,
    };
    KafkaError::new(kind, error.to_string())
}

fn translate_metadata(
    topic: Arc<str>,
    create_timestamp: i64,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
    metadata: EngineRecordMetadata,
) -> Result<RecordMetadata, KafkaError> {
    metadata_parts(
        topic,
        metadata.partition(),
        metadata.offset(),
        metadata.append_timestamp().or(Some(create_timestamp)),
        metadata.leader_epoch(),
        serialized_key_size,
        serialized_value_size,
    )
}

pub(super) fn metadata_parts(
    topic: impl Into<Arc<str>>,
    partition: u32,
    offset: i64,
    append_timestamp: Option<i64>,
    leader_epoch: Option<i32>,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
) -> Result<RecordMetadata, KafkaError> {
    let Ok(partition) = i32::try_from(partition) else {
        return Err(KafkaError::new(
            ErrorKind::Internal,
            "engine returned a producer partition outside Kafka's signed range",
        ));
    };
    Ok(RecordMetadata::from_parts(
        topic,
        partition,
        offset,
        append_timestamp,
        leader_epoch,
        serialized_key_size,
        serialized_value_size,
    ))
}
