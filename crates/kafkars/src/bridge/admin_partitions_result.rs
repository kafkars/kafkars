//! Exhaustive stable translation of concrete engine partition outcomes.

use kafka_client_engine::{
    CreatePartitionsAcceptedFaultKind, CreatePartitionsAdmissionError,
    CreatePartitionsAdmissionErrorKind, CreatePartitionsDeliveryStatus, CreatePartitionsFailure,
    CreatePartitionsFailureKind, CreatePartitionsObserverError, CreatePartitionsOutcome,
    PartitionIncreaseError as EngineTopicError,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult,
    bridge::admin_partitions_operation::AdminCreatePartitionsResult,
};

pub(super) fn translate_admission_error(error: CreatePartitionsAdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: CreatePartitionsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        CreatePartitionsAdmissionErrorKind::InvalidRequest
        | CreatePartitionsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        CreatePartitionsAdmissionErrorKind::Contended
        | CreatePartitionsAdmissionErrorKind::Capacity
        | CreatePartitionsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        CreatePartitionsAdmissionErrorKind::Closed => ErrorKind::State,
        CreatePartitionsAdmissionErrorKind::IdentityExhausted
        | CreatePartitionsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    let error = KafkaError::new(
        public,
        format!("CreatePartitions admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent);
    match kind {
        CreatePartitionsAdmissionErrorKind::Contended
        | CreatePartitionsAdmissionErrorKind::Capacity
        | CreatePartitionsAdmissionErrorKind::RetainedBytes => error.with_safe_retry(),
        CreatePartitionsAdmissionErrorKind::InvalidRequest
        | CreatePartitionsAdmissionErrorKind::InvalidDeadline
        | CreatePartitionsAdmissionErrorKind::Closed
        | CreatePartitionsAdmissionErrorKind::IdentityExhausted
        | CreatePartitionsAdmissionErrorKind::HostUnavailable => error,
    }
}

pub(super) fn translate_accepted_fault(fault: CreatePartitionsAcceptedFaultKind) -> KafkaError {
    match fault {
        CreatePartitionsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "CreatePartitions was accepted but its host wake failed",
        ),
        CreatePartitionsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "CreatePartitions was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<CreatePartitionsOutcome, CreatePartitionsObserverError>,
) -> AdminCreatePartitionsResult {
    match result {
        Ok(CreatePartitionsOutcome::Topics(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (name, result) = topic.into_parts();
                    (name, result.map_err(translate_topic_error))
                })
                .collect(),
        )),
        Ok(CreatePartitionsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_failure(failure: CreatePartitionsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: CreatePartitionsFailureKind,
    delivery: CreatePartitionsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        CreatePartitionsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        CreatePartitionsFailureKind::DriverRejected => ErrorKind::Backpressure,
        CreatePartitionsFailureKind::Transport => ErrorKind::Transport,
        CreatePartitionsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("CreatePartitions failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

fn translate_topic_error(error: EngineTopicError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_topic_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_topic_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected CreatePartitions with broker code {code}"),
        |message| format!("Kafka rejected CreatePartitions with broker code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_diagnostic_truncated(message_truncated)
}

const fn translate_delivery(delivery: CreatePartitionsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        CreatePartitionsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        CreatePartitionsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: CreatePartitionsObserverError) -> KafkaError {
    let public = match error {
        CreatePartitionsObserverError::AlreadyObserved => ErrorKind::State,
        CreatePartitionsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
