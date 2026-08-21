//! Exhaustive translation of concrete leader election outcomes.

use std::time::Duration;

use kafka_client_engine::{
    ElectLeadersAcceptedFaultKind, ElectLeadersAdmissionError, ElectLeadersAdmissionErrorKind,
    ElectLeadersDeliveryStatus, ElectLeadersFailure, ElectLeadersFailureKind,
    ElectLeadersObserverError, ElectLeadersOutcome, LeaderElectionBrokerError as EngineBrokerError,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, ElectLeadersResult},
};

use super::operation::AdminElectLeadersResult;

pub(super) fn translate_admission_error(error: &ElectLeadersAdmissionError) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        ElectLeadersAdmissionErrorKind::InvalidRequest
        | ElectLeadersAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        ElectLeadersAdmissionErrorKind::Contended
        | ElectLeadersAdmissionErrorKind::Capacity
        | ElectLeadersAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        ElectLeadersAdmissionErrorKind::Closed => ErrorKind::State,
        ElectLeadersAdmissionErrorKind::IdentityExhausted
        | ElectLeadersAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("ElectLeaders admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: ElectLeadersAcceptedFaultKind) -> KafkaError {
    match fault {
        ElectLeadersAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ElectLeaders was accepted but its host wake failed",
        ),
        ElectLeadersAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ElectLeaders host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<ElectLeadersOutcome, ElectLeadersObserverError>,
) -> AdminElectLeadersResult {
    match result {
        Ok(ElectLeadersOutcome::Elected(batch)) => {
            let (throttle_time_ms, partitions) = batch.into_parts();
            let entries = partitions
                .into_iter()
                .map(|partition| {
                    let (topic, partition, result) = partition.into_parts();
                    (
                        TopicPartition::new(topic, partition),
                        result.map_err(partition_error),
                    )
                })
                .collect();
            Ok(ElectLeadersResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(ElectLeadersOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn partition_error(error: EngineBrokerError) -> KafkaError {
    broker_error("partition", error, DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: ElectLeadersFailure) -> KafkaError {
    let (kind, delivery) = failure.into_parts();
    let delivery = translate_delivery(delivery);
    match kind {
        ElectLeadersFailureKind::Broker(error) => broker_error("request", error, delivery),
        kind => {
            let public = match kind {
                ElectLeadersFailureKind::DeadlineElapsed => ErrorKind::Timeout,
                ElectLeadersFailureKind::DriverRejected
                | ElectLeadersFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
                ElectLeadersFailureKind::Transport => ErrorKind::Transport,
                ElectLeadersFailureKind::Compatibility => ErrorKind::Compatibility,
                ElectLeadersFailureKind::InvalidResponse => ErrorKind::Broker,
                ElectLeadersFailureKind::Broker(_) => unreachable!(),
            };
            KafkaError::new(public, format!("ElectLeaders failed: {kind:?}"))
                .with_delivery_status(delivery)
        }
    }
}

fn broker_error(scope: &str, error: EngineBrokerError, delivery: DeliveryStatus) -> KafkaError {
    let (code, message, truncated) = error.into_parts();
    broker_error_parts(scope, code, message.as_deref(), truncated, delivery)
}

pub(super) fn broker_error_parts(
    scope: &str,
    code: i16,
    message: Option<&str>,
    truncated: bool,
    delivery: DeliveryStatus,
) -> KafkaError {
    let diagnostic = message.map_or_else(
        || format!("Kafka rejected leader election {scope} with broker code {code}"),
        |message| {
            let suffix = if truncated { " (truncated)" } else { "" };
            format!(
                "Kafka rejected leader election {scope} with broker code {code}: {message}{suffix}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
        .with_diagnostic_truncated(truncated)
}

const fn translate_delivery(delivery: ElectLeadersDeliveryStatus) -> DeliveryStatus {
    match delivery {
        ElectLeadersDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        ElectLeadersDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: ElectLeadersObserverError) -> KafkaError {
    let public = match error {
        ElectLeadersObserverError::AlreadyObserved => ErrorKind::State,
        ElectLeadersObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
