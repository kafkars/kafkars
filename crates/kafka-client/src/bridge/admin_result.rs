//! Exhaustive stable translation of concrete engine admin outcomes.

use kafka_client_engine::{
    CreateTopicError as EngineTopicError, CreateTopicsAcceptedFaultKind,
    CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind, CreateTopicsDeliveryStatus,
    CreateTopicsFailure, CreateTopicsFailureKind, CreateTopicsObserverError, CreateTopicsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult,
    bridge::admin_operation::AdminCreateTopicsResult,
};

pub(super) fn translate_admission_error(error: CreateTopicsAdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_accepted_fault(fault: CreateTopicsAcceptedFaultKind) -> KafkaError {
    match fault {
        CreateTopicsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "CreateTopics was accepted but its host wake failed",
        ),
        CreateTopicsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "CreateTopics was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<CreateTopicsOutcome, CreateTopicsObserverError>,
) -> AdminCreateTopicsResult {
    match result {
        Ok(CreateTopicsOutcome::Topics(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (name, result) = topic.into_parts();
                    let result = match result {
                        Ok(()) => Ok(()),
                        Err(error) => Err(translate_topic_error(error)),
                    };
                    (name, result)
                })
                .collect(),
        )),
        Ok(CreateTopicsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) fn translate_admission_kind(kind: CreateTopicsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        CreateTopicsAdmissionErrorKind::InvalidRequest
        | CreateTopicsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        CreateTopicsAdmissionErrorKind::Contended
        | CreateTopicsAdmissionErrorKind::Capacity
        | CreateTopicsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        CreateTopicsAdmissionErrorKind::Closed => ErrorKind::State,
        CreateTopicsAdmissionErrorKind::IdentityExhausted
        | CreateTopicsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("CreateTopics admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

fn translate_failure(failure: CreateTopicsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: CreateTopicsFailureKind,
    delivery: CreateTopicsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        CreateTopicsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        CreateTopicsFailureKind::DriverRejected => ErrorKind::Backpressure,
        CreateTopicsFailureKind::Transport => ErrorKind::Transport,
        CreateTopicsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("CreateTopics failed: {kind:?}"))
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
        || format!("Kafka rejected CreateTopics with broker code {code}"),
        |message| format!("Kafka rejected CreateTopics with broker code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_diagnostic_truncated(message_truncated)
}

const fn translate_delivery(delivery: CreateTopicsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        CreateTopicsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        CreateTopicsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: CreateTopicsObserverError) -> KafkaError {
    let public = match error {
        CreateTopicsObserverError::AlreadyObserved => ErrorKind::State,
        CreateTopicsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
