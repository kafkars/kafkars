//! Exhaustive stable translation of concrete engine incremental configuration outcomes.

use std::time::Duration;

use kafka_client_engine::{
    IncrementalAlterConfigError as EngineTopicError, IncrementalAlterConfigsAcceptedFaultKind,
    IncrementalAlterConfigsAdmissionError, IncrementalAlterConfigsAdmissionErrorKind,
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsObserverError,
    IncrementalAlterConfigsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{
        BatchResult, ConfigResource, ConfigResourceType, IncrementalAlterConfigResourcesResult,
        IncrementalAlterConfigsResult,
    },
    bridge::{
        admin_alter_config_resources_operation::AdminIncrementalAlterConfigResourcesResult,
        admin_alter_configs_operation::AdminIncrementalAlterConfigsResult,
    },
};

pub(super) fn translate_admission_error(
    error: IncrementalAlterConfigsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(
    kind: IncrementalAlterConfigsAdmissionErrorKind,
) -> KafkaError {
    let public = match kind {
        IncrementalAlterConfigsAdmissionErrorKind::InvalidRequest
        | IncrementalAlterConfigsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        IncrementalAlterConfigsAdmissionErrorKind::Contended
        | IncrementalAlterConfigsAdmissionErrorKind::Capacity
        | IncrementalAlterConfigsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        IncrementalAlterConfigsAdmissionErrorKind::Closed => ErrorKind::State,
        IncrementalAlterConfigsAdmissionErrorKind::IdentityExhausted
        | IncrementalAlterConfigsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("IncrementalAlterConfigs admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: IncrementalAlterConfigsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        IncrementalAlterConfigsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "IncrementalAlterConfigs was accepted but its host wake failed",
        ),
        IncrementalAlterConfigsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "IncrementalAlterConfigs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<IncrementalAlterConfigsOutcome, IncrementalAlterConfigsObserverError>,
) -> AdminIncrementalAlterConfigsResult {
    match result {
        Ok(IncrementalAlterConfigsOutcome::Configs(batch)) => {
            let (throttle_time_ms, topics) = batch.into_parts();
            let entries = topics
                .into_iter()
                .map(|topic| {
                    let (topic, result) = topic.into_parts();
                    (topic, result.map_err(translate_topic_error))
                })
                .collect();
            Ok(IncrementalAlterConfigsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(IncrementalAlterConfigsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(super) fn translate_resource_observation(
    result: Result<IncrementalAlterConfigsOutcome, IncrementalAlterConfigsObserverError>,
) -> AdminIncrementalAlterConfigResourcesResult {
    match result {
        Ok(IncrementalAlterConfigsOutcome::Configs(batch)) => {
            let (throttle_time_ms, resources) = batch.into_parts();
            let entries = resources
                .into_iter()
                .map(|resource| {
                    let (resource_type, resource_name, result) = resource.into_resource_parts();
                    (
                        ConfigResource::new(
                            ConfigResourceType::from_engine(resource_type),
                            resource_name,
                        ),
                        result.map_err(translate_resource_error),
                    )
                })
                .collect();
            Ok(IncrementalAlterConfigResourcesResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(IncrementalAlterConfigsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_resource_error(error: EngineTopicError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    let detail = message.map_or_else(
        || format!("Kafka rejected IncrementalAlterConfigs with broker code {code}"),
        |message| {
            format!("Kafka rejected IncrementalAlterConfigs with broker code {code}: {message}")
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
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
        || format!("Kafka rejected topic IncrementalAlterConfigs with broker code {code}"),
        |message| {
            format!(
                "Kafka rejected topic IncrementalAlterConfigs with broker code {code}: {message}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: IncrementalAlterConfigsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: IncrementalAlterConfigsFailureKind,
    delivery: IncrementalAlterConfigsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        IncrementalAlterConfigsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        IncrementalAlterConfigsFailureKind::DriverRejected
        | IncrementalAlterConfigsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        IncrementalAlterConfigsFailureKind::Transport => ErrorKind::Transport,
        IncrementalAlterConfigsFailureKind::InvalidResponse => ErrorKind::Broker,
        IncrementalAlterConfigsFailureKind::Compatibility => ErrorKind::Compatibility,
    };
    KafkaError::new(public, format!("IncrementalAlterConfigs failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: IncrementalAlterConfigsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        IncrementalAlterConfigsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        IncrementalAlterConfigsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: IncrementalAlterConfigsObserverError) -> KafkaError {
    let public = match error {
        IncrementalAlterConfigsObserverError::AlreadyObserved => ErrorKind::State,
        IncrementalAlterConfigsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
