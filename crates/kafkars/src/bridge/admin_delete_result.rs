//! Exhaustive stable translation of concrete engine deletion outcomes.

use kafka_client_engine::{
    DeleteTopicError as EngineTopicError, DeleteTopicsAcceptedFaultKind,
    DeleteTopicsAdmissionError, DeleteTopicsAdmissionErrorKind, DeleteTopicsDeliveryStatus,
    DeleteTopicsFailure, DeleteTopicsFailureKind, DeleteTopicsObserverError, DeleteTopicsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, admin::BatchResult,
    bridge::admin_delete_operation::AdminDeleteTopicsResult,
};

pub(in crate::bridge) fn translate_admission_error(
    error: DeleteTopicsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: DeleteTopicsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        DeleteTopicsAdmissionErrorKind::InvalidRequest
        | DeleteTopicsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DeleteTopicsAdmissionErrorKind::Contended
        | DeleteTopicsAdmissionErrorKind::Capacity
        | DeleteTopicsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DeleteTopicsAdmissionErrorKind::Closed => ErrorKind::State,
        DeleteTopicsAdmissionErrorKind::IdentityExhausted
        | DeleteTopicsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("DeleteTopics admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(in crate::bridge) fn translate_accepted_fault(
    fault: DeleteTopicsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        DeleteTopicsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteTopics was accepted but its host wake failed",
        ),
        DeleteTopicsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteTopics was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DeleteTopicsOutcome, DeleteTopicsObserverError>,
) -> AdminDeleteTopicsResult {
    match result {
        Ok(DeleteTopicsOutcome::Topics(topics)) => Ok(BatchResult::new(
            topics
                .into_iter()
                .map(|topic| {
                    let (name, result) = topic.into_parts();
                    (name, result.map_err(translate_topic_error))
                })
                .collect(),
        )),
        Ok(DeleteTopicsOutcome::TopicIds(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "name-based DeleteTopics received a topic-ID terminal",
        )
        .with_delivery_status(DeliveryStatus::PossiblySent)),
        Ok(DeleteTopicsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(in crate::bridge) fn translate_failure(failure: DeleteTopicsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: DeleteTopicsFailureKind,
    delivery: DeleteTopicsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        DeleteTopicsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DeleteTopicsFailureKind::DriverRejected => ErrorKind::Backpressure,
        DeleteTopicsFailureKind::Transport => ErrorKind::Transport,
        DeleteTopicsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DeleteTopics failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

pub(in crate::bridge) fn translate_topic_error(error: EngineTopicError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_topic_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_topic_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected DeleteTopics with broker code {code}"),
        |message| format!("Kafka rejected DeleteTopics with broker code {code}: {message}"),
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_diagnostic_truncated(message_truncated)
}

const fn translate_delivery(delivery: DeleteTopicsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DeleteTopicsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DeleteTopicsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(in crate::bridge) fn translate_observer_error(error: DeleteTopicsObserverError) -> KafkaError {
    let public = match error {
        DeleteTopicsObserverError::AlreadyObserved => ErrorKind::State,
        DeleteTopicsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
