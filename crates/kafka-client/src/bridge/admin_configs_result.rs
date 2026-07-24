//! Exhaustive stable translation of concrete engine topic configuration outcomes.

use std::time::Duration;

use kafka_client_engine::{
    DescribeConfigEntry as EngineConfigEntry, DescribeConfigResourceError as EngineResourceError,
    DescribeConfigSynonym as EngineConfigSynonym, DescribeConfigsAcceptedFaultKind,
    DescribeConfigsAdmissionError, DescribeConfigsAdmissionErrorKind,
    DescribeConfigsDeliveryStatus, DescribeConfigsFailure, DescribeConfigsFailureKind,
    DescribeConfigsObserverError, DescribeConfigsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, ConfigEntry, ConfigSynonym, DescribeConfigsResult},
    bridge::admin_configs_operation::AdminDescribeConfigsResult,
};

pub(super) fn translate_admission_error(error: DescribeConfigsAdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: DescribeConfigsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        DescribeConfigsAdmissionErrorKind::InvalidRequest
        | DescribeConfigsAdmissionErrorKind::UnsupportedResource
        | DescribeConfigsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DescribeConfigsAdmissionErrorKind::Contended
        | DescribeConfigsAdmissionErrorKind::Capacity
        | DescribeConfigsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DescribeConfigsAdmissionErrorKind::Closed => ErrorKind::State,
        DescribeConfigsAdmissionErrorKind::IdentityExhausted
        | DescribeConfigsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DescribeConfigs admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: DescribeConfigsAcceptedFaultKind) -> KafkaError {
    match fault {
        DescribeConfigsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeConfigs was accepted but its host wake failed",
        ),
        DescribeConfigsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeConfigs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DescribeConfigsOutcome, DescribeConfigsObserverError>,
) -> AdminDescribeConfigsResult {
    match result {
        Ok(DescribeConfigsOutcome::Configs(batch)) => {
            let throttle = Duration::from_millis(u64::from(batch.throttle_time_ms()));
            let entries = batch
                .into_resources()
                .into_iter()
                .map(|resource| {
                    let (_resource_type, name, result) = resource.into_parts();
                    (
                        name,
                        result
                            .map(translate_entries)
                            .map_err(translate_resource_error),
                    )
                })
                .collect();
            Ok(DescribeConfigsResult::new(
                throttle,
                BatchResult::new(entries),
            ))
        }
        Ok(DescribeConfigsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_entries(entries: Vec<EngineConfigEntry>) -> Vec<ConfigEntry> {
    entries.into_iter().map(translate_entry).collect()
}

fn translate_entry(entry: EngineConfigEntry) -> ConfigEntry {
    let (name, value, read_only, source, sensitive, synonyms, config_type, documentation) =
        entry.into_parts();
    ConfigEntry::new(
        name,
        value,
        read_only,
        source,
        sensitive,
        synonyms.into_iter().map(translate_synonym).collect(),
        config_type,
        documentation,
    )
}

fn translate_synonym(synonym: EngineConfigSynonym) -> ConfigSynonym {
    let (name, value, source) = synonym.into_parts();
    ConfigSynonym::new(name, value, source)
}

fn translate_resource_error(error: EngineResourceError) -> KafkaError {
    let (code, message, message_truncated) = error.into_parts();
    translate_resource_error_parts(code, message.as_deref(), message_truncated)
}

pub(super) fn translate_resource_error_parts(
    code: i16,
    message: Option<&str>,
    message_truncated: bool,
) -> KafkaError {
    let detail = message.map_or_else(
        || format!("Kafka rejected topic DescribeConfigs with broker code {code}"),
        |message| {
            format!("Kafka rejected topic DescribeConfigs with broker code {code}: {message}")
        },
    );
    KafkaError::new(ErrorKind::Broker, detail)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: DescribeConfigsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: DescribeConfigsFailureKind,
    delivery: DescribeConfigsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        DescribeConfigsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DescribeConfigsFailureKind::DriverRejected
        | DescribeConfigsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        DescribeConfigsFailureKind::Transport => ErrorKind::Transport,
        DescribeConfigsFailureKind::InvalidResponse => ErrorKind::Broker,
        DescribeConfigsFailureKind::Compatibility => ErrorKind::Compatibility,
    };
    KafkaError::new(public, format!("DescribeConfigs failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DescribeConfigsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DescribeConfigsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DescribeConfigsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: DescribeConfigsObserverError) -> KafkaError {
    let public = match error {
        DescribeConfigsObserverError::AlreadyObserved => ErrorKind::State,
        DescribeConfigsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
