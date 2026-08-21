//! Exhaustive stable translation of legacy full-snapshot replacement outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        BatchResult, ConfigResource, ConfigResourceType,
        LegacyReplaceConfigResourcesResult as PublicResourceResult,
        LegacyReplaceTopicConfigsResult as PublicResult,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, DeliveryStatus, Failure,
        FailureKind, ObserverError, Outcome, Result as EngineResult, TopicError, TopicResult,
    },
    operation::AdminLegacyReplaceTopicConfigsResult,
    resource_operation::AdminLegacyReplaceConfigResourcesResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_resource_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminLegacyReplaceConfigResourcesResult {
    match result {
        Ok(Outcome::Configs(result)) => Ok(translate_resource_result(result)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_resource_result(result: EngineResult) -> PublicResourceResult {
    let (throttle_time_ms, resources) = result.into_parts();
    PublicResourceResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(
            resources
                .into_iter()
                .map(translate_resource_result_entry)
                .collect(),
        ),
    )
}

fn translate_resource_result_entry(
    resource: TopicResult,
) -> (ConfigResource, Result<(), KafkaError>) {
    let (resource_type, resource_name, result) = resource.into_resource_parts();
    (
        ConfigResource::new(
            ConfigResourceType::from_engine(resource_type),
            resource_name,
        ),
        result.map_err(translate_resource_error),
    )
}

fn translate_resource_error(error: TopicError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    let detail = message.map_or_else(
        || format!("Kafka rejected legacy configuration replacement with broker code {code}"),
        |message| {
            format!(
                "Kafka rejected legacy configuration replacement with broker code {code}: \
                 {message}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
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
        format!("LegacyReplaceTopicConfigs admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "LegacyReplaceTopicConfigs was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "LegacyReplaceTopicConfigs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminLegacyReplaceTopicConfigsResult {
    match result {
        Ok(Outcome::Configs(result)) => Ok(translate_result(result)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_result(result: EngineResult) -> PublicResult {
    let (throttle_time_ms, topics) = result.into_parts();
    PublicResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(topics.into_iter().map(translate_topic_result).collect()),
    )
}

fn translate_topic_result(topic: TopicResult) -> (String, Result<(), KafkaError>) {
    let (topic, result) = topic.into_parts();
    (topic, result.map_err(translate_topic_error))
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
    let detail = message.map_or_else(
        || format!("Kafka rejected legacy topic configuration replacement with broker code {code}"),
        |message| {
            format!(
                "Kafka rejected legacy topic configuration replacement with broker code \
                 {code}: {message}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
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
    KafkaError::new(
        public,
        format!("LegacyReplaceTopicConfigs failed: {kind:?}"),
    )
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
