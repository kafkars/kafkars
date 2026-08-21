//! Exhaustive translation of concrete reassignment outcomes.

use std::time::Duration;

use kafka_client_engine::{
    AlterPartitionReassignmentBrokerError as EngineBrokerError,
    AlterPartitionReassignmentsAcceptedFaultKind, AlterPartitionReassignmentsAdmissionError,
    AlterPartitionReassignmentsAdmissionErrorKind, AlterPartitionReassignmentsDeliveryStatus,
    AlterPartitionReassignmentsFailure, AlterPartitionReassignmentsFailureKind,
    AlterPartitionReassignmentsObserverError, AlterPartitionReassignmentsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{AlterPartitionReassignmentsResult, BatchResult},
};

use super::alter_operation::AdminAlterPartitionReassignmentsResult;

pub(super) fn translate_admission_error(
    error: &AlterPartitionReassignmentsAdmissionError,
) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        AlterPartitionReassignmentsAdmissionErrorKind::InvalidRequest
        | AlterPartitionReassignmentsAdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AlterPartitionReassignmentsAdmissionErrorKind::Contended
        | AlterPartitionReassignmentsAdmissionErrorKind::Capacity
        | AlterPartitionReassignmentsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AlterPartitionReassignmentsAdmissionErrorKind::Closed => ErrorKind::State,
        AlterPartitionReassignmentsAdmissionErrorKind::IdentityExhausted
        | AlterPartitionReassignmentsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("AlterPartitionReassignments admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: AlterPartitionReassignmentsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        AlterPartitionReassignmentsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "AlterPartitionReassignments was accepted but its host wake failed",
        ),
        AlterPartitionReassignmentsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "AlterPartitionReassignments host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<AlterPartitionReassignmentsOutcome, AlterPartitionReassignmentsObserverError>,
) -> AdminAlterPartitionReassignmentsResult {
    match result {
        Ok(AlterPartitionReassignmentsOutcome::Altered(batch)) => {
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
            Ok(AlterPartitionReassignmentsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(AlterPartitionReassignmentsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn partition_error(error: EngineBrokerError) -> KafkaError {
    broker_error("partition", error, DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: AlterPartitionReassignmentsFailure) -> KafkaError {
    let (kind, delivery) = failure.into_parts();
    let delivery = translate_delivery(delivery);
    match kind {
        AlterPartitionReassignmentsFailureKind::Broker(error) => {
            broker_error("request", error, delivery)
        }
        kind => {
            let public = match kind {
                AlterPartitionReassignmentsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
                AlterPartitionReassignmentsFailureKind::DriverRejected
                | AlterPartitionReassignmentsFailureKind::ResponseTooLarge => {
                    ErrorKind::Backpressure
                }
                AlterPartitionReassignmentsFailureKind::Transport => ErrorKind::Transport,
                AlterPartitionReassignmentsFailureKind::Compatibility => ErrorKind::Compatibility,
                AlterPartitionReassignmentsFailureKind::InvalidResponse => ErrorKind::Broker,
                AlterPartitionReassignmentsFailureKind::Broker(_) => unreachable!(),
            };
            KafkaError::new(
                public,
                format!("AlterPartitionReassignments failed: {kind:?}"),
            )
            .with_delivery_status(delivery)
        }
    }
}

fn broker_error(scope: &str, error: EngineBrokerError, delivery: DeliveryStatus) -> KafkaError {
    let (code, message, truncated) = error.into_parts();
    translate_broker_parts(scope, code, message, truncated, delivery)
}

pub(super) fn translate_broker_parts(
    scope: &str,
    code: i16,
    message: Option<String>,
    diagnostic_truncated: bool,
    delivery: DeliveryStatus,
) -> KafkaError {
    let diagnostic = message.map_or_else(
        || format!("Kafka rejected reassignment {scope} with broker code {code}"),
        |message| {
            let suffix = if diagnostic_truncated {
                " (truncated)"
            } else {
                ""
            };
            format!(
                "Kafka rejected reassignment {scope} with broker code {code}: {message}{suffix}"
            )
        },
    );
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
        .with_diagnostic_truncated(diagnostic_truncated)
}

const fn translate_delivery(delivery: AlterPartitionReassignmentsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        AlterPartitionReassignmentsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        AlterPartitionReassignmentsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: AlterPartitionReassignmentsObserverError) -> KafkaError {
    let public = match error {
        AlterPartitionReassignmentsObserverError::AlreadyObserved => ErrorKind::State,
        AlterPartitionReassignmentsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
