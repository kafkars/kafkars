//! Exhaustive stable translation of concrete engine reassignment outcomes.

use std::time::Duration;

use kafka_client_engine::{
    ListPartitionReassignmentsAcceptedFaultKind, ListPartitionReassignmentsAdmissionError,
    ListPartitionReassignmentsAdmissionErrorKind, ListPartitionReassignmentsDeliveryStatus,
    ListPartitionReassignmentsFailure, ListPartitionReassignmentsFailureKind,
    ListPartitionReassignmentsObserverError, ListPartitionReassignmentsOutcome,
    PartitionReassignment as EngineReassignment,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{ListPartitionReassignmentsResult, PartitionReassignment},
};

use super::operation::AdminListPartitionReassignmentsResult;

pub(super) fn translate_admission_error(
    error: ListPartitionReassignmentsAdmissionError,
) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        ListPartitionReassignmentsAdmissionErrorKind::InvalidRequest
        | ListPartitionReassignmentsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        ListPartitionReassignmentsAdmissionErrorKind::Contended
        | ListPartitionReassignmentsAdmissionErrorKind::Capacity
        | ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        ListPartitionReassignmentsAdmissionErrorKind::Closed => ErrorKind::State,
        ListPartitionReassignmentsAdmissionErrorKind::IdentityExhausted
        | ListPartitionReassignmentsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("ListPartitionReassignments admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: ListPartitionReassignmentsAcceptedFaultKind,
) -> KafkaError {
    let message = match fault {
        ListPartitionReassignmentsAcceptedFaultKind::Wake => {
            "ListPartitionReassignments was accepted but its host wake failed"
        }
        ListPartitionReassignmentsAcceptedFaultKind::HostInvariant => {
            "ListPartitionReassignments was accepted but its host reported an invariant failure"
        }
    };
    KafkaError::new(ErrorKind::Internal, message)
}

pub(super) fn translate_observation(
    result: Result<ListPartitionReassignmentsOutcome, ListPartitionReassignmentsObserverError>,
) -> AdminListPartitionReassignmentsResult {
    match result {
        Ok(ListPartitionReassignmentsOutcome::Reassignments(batch)) => {
            let (throttle_time_ms, reassignments) = batch.into_parts();
            let rows = reassignments
                .into_iter()
                .map(|row| {
                    let (topic, partition, reassignment) = row.into_parts();
                    (
                        TopicPartition::new(topic, partition),
                        translate_reassignment(reassignment),
                    )
                })
                .collect();
            Ok(ListPartitionReassignmentsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                rows,
            ))
        }
        Ok(ListPartitionReassignmentsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_reassignment(reassignment: EngineReassignment) -> PartitionReassignment {
    let (replicas, adding_replicas, removing_replicas) = reassignment.into_parts();
    PartitionReassignment::new(replicas, adding_replicas, removing_replicas)
}

fn translate_failure(failure: ListPartitionReassignmentsFailure) -> KafkaError {
    let (kind, delivery) = failure.into_parts();
    let delivery = translate_delivery(delivery);
    let (public, broker_code) = match kind {
        ListPartitionReassignmentsFailureKind::DeadlineElapsed => (ErrorKind::Timeout, None),
        ListPartitionReassignmentsFailureKind::DriverRejected
        | ListPartitionReassignmentsFailureKind::ResponseTooLarge => {
            (ErrorKind::Backpressure, None)
        }
        ListPartitionReassignmentsFailureKind::Transport => (ErrorKind::Transport, None),
        ListPartitionReassignmentsFailureKind::Broker(error) => {
            let (code, message, truncated) = error.into_parts();
            return translate_broker_parts(code, message, truncated, delivery);
        }
        ListPartitionReassignmentsFailureKind::Compatibility => (ErrorKind::Compatibility, None),
        ListPartitionReassignmentsFailureKind::InvalidResponse => (ErrorKind::Broker, None),
    };
    KafkaError::new(
        public,
        format!("ListPartitionReassignments failed: {public:?}"),
    )
    .with_broker_code(broker_code)
    .with_delivery_status(delivery)
}

pub(super) fn translate_broker_parts(
    code: i16,
    message: Option<String>,
    diagnostic_truncated: bool,
    delivery: DeliveryStatus,
) -> KafkaError {
    let diagnostic = message.map(|message| {
        if diagnostic_truncated {
            format!("{message} [truncated]")
        } else {
            message
        }
    });
    let message = diagnostic
        .unwrap_or_else(|| format!("ListPartitionReassignments failed: {:?}", ErrorKind::Broker));
    KafkaError::new(ErrorKind::Broker, message)
        .with_broker_code(Some(code))
        .with_delivery_status(delivery)
        .with_diagnostic_truncated(diagnostic_truncated)
}

const fn translate_delivery(delivery: ListPartitionReassignmentsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        ListPartitionReassignmentsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        ListPartitionReassignmentsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: ListPartitionReassignmentsObserverError) -> KafkaError {
    let public = match error {
        ListPartitionReassignmentsObserverError::AlreadyObserved => ErrorKind::State,
        ListPartitionReassignmentsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
