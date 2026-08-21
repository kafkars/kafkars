//! Exhaustive stable translation of engine `ShareGroup` offset-deletion outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, DeleteShareGroupOffsetsResult as PublicResult},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, TopicError, TopicResult,
    },
    operation::AdminDeleteShareGroupOffsetsResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DeleteShareGroupOffsets admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteShareGroupOffsets was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteShareGroupOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDeleteShareGroupOffsetsResult {
    match result {
        Ok(Outcome::Deleted(batch)) => {
            let (throttle_time_ms, topics) = batch.into_parts();
            let entries = topics.into_iter().map(translate_topic_result).collect();
            Ok(PublicResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_topic_result(topic: TopicResult) -> (String, Result<[u8; 16], KafkaError>) {
    let (name, result) = topic.into_parts();
    (name, result.map_err(translate_topic_error))
}

fn translate_topic_error(error: TopicError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_topic_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_topic_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    broker_error(
        "Kafka rejected DeleteShareGroupOffsets topic",
        code,
        message,
        message_truncated,
    )
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    let context =
        format!("Kafka rejected DeleteShareGroupOffsets after {throttle_time_ms} ms throttle");
    broker_error(&context, code, message.as_deref(), message_truncated)
}

fn broker_error(
    context: &str,
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => format!("{context} with broker code {code}: {message}"),
        None => format!("{context} with broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DeleteShareGroupOffsets failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
