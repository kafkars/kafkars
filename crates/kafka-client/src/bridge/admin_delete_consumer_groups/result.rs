//! Exhaustive stable translation of concrete engine Admin `DeleteConsumerGroups` outcomes.

use std::time::Duration;

use kafka_client_engine::{
    DeleteConsumerGroupsAcceptedFaultKind, DeleteConsumerGroupsAdmissionError,
    DeleteConsumerGroupsAdmissionErrorKind, DeleteConsumerGroupsDeliveryStatus,
    DeleteConsumerGroupsFailure, DeleteConsumerGroupsFailureKind,
    DeleteConsumerGroupsObserverError, DeleteConsumerGroupsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{BatchResult, DeleteConsumerGroupsResult},
};

use super::operation::AdminDeleteConsumerGroupsResult;

pub(super) fn translate_admission_error(error: DeleteConsumerGroupsAdmissionError) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        DeleteConsumerGroupsAdmissionErrorKind::InvalidRequest
        | DeleteConsumerGroupsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DeleteConsumerGroupsAdmissionErrorKind::Contended
        | DeleteConsumerGroupsAdmissionErrorKind::Capacity
        | DeleteConsumerGroupsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DeleteConsumerGroupsAdmissionErrorKind::Closed => ErrorKind::State,
        DeleteConsumerGroupsAdmissionErrorKind::IdentityExhausted
        | DeleteConsumerGroupsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DeleteConsumerGroups admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: DeleteConsumerGroupsAcceptedFaultKind) -> KafkaError {
    match fault {
        DeleteConsumerGroupsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteConsumerGroups was accepted but its host wake failed",
        ),
        DeleteConsumerGroupsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteConsumerGroups was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DeleteConsumerGroupsOutcome, DeleteConsumerGroupsObserverError>,
) -> AdminDeleteConsumerGroupsResult {
    match result {
        Ok(DeleteConsumerGroupsOutcome::Deleted(batch)) => {
            let (throttle_time_ms, groups) = batch.into_parts();
            let entries = groups.into_iter().map(translate_group).collect();
            Ok(DeleteConsumerGroupsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(DeleteConsumerGroupsOutcome::Failed(failure)) => Ok(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_group(
    group: kafka_client_engine::DeleteConsumerGroupsEngineResult,
) -> (String, Result<(), KafkaError>) {
    let (group_id, result) = group.into_parts();
    (
        group_id,
        result
            .map(translate_result)
            .map_err(|error| translate_group_error(error.into_parts())),
    )
}

fn translate_result(_deleted: ()) {}

pub(super) fn translate_group_error(
    (code, message, message_truncated): (i16, Option<String>, bool),
) -> KafkaError {
    let diagnostic = match message {
        Some(message) => {
            format!("Kafka returned DeleteConsumerGroups group broker code {code}: {message}")
        }
        None => format!("Kafka returned DeleteConsumerGroups group broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(DeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

fn translate_failure(failure: DeleteConsumerGroupsFailure) -> DeleteConsumerGroupsResult {
    let (kind, delivery, throttle_time_ms, completed, failed_group, unattempted) =
        failure.into_parts();
    partial_result(
        throttle_time_ms,
        completed.into_iter().map(translate_group).collect(),
        failed_group,
        translate_attempt_failure(kind, delivery),
        unattempted,
    )
}

fn translate_attempt_failure(
    kind: DeleteConsumerGroupsFailureKind,
    delivery: DeleteConsumerGroupsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        DeleteConsumerGroupsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DeleteConsumerGroupsFailureKind::DriverRejected
        | DeleteConsumerGroupsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        DeleteConsumerGroupsFailureKind::Transport => ErrorKind::Transport,
        DeleteConsumerGroupsFailureKind::Compatibility => ErrorKind::Compatibility,
        DeleteConsumerGroupsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DeleteConsumerGroups failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

pub(super) fn partial_result(
    throttle_time_ms: u32,
    mut completed: Vec<(String, Result<(), KafkaError>)>,
    failed_group: String,
    failed_error: KafkaError,
    unattempted: Vec<String>,
) -> DeleteConsumerGroupsResult {
    completed.reserve(1 + unattempted.len());
    completed.push((failed_group, Err(failed_error)));
    completed.extend(
        unattempted
            .into_iter()
            .map(|group_id| (group_id, Err(unattempted_error()))),
    );
    DeleteConsumerGroupsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(completed),
    )
}

fn unattempted_error() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "DeleteConsumerGroups target was not attempted because an earlier target failed",
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

const fn translate_delivery(delivery: DeleteConsumerGroupsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DeleteConsumerGroupsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DeleteConsumerGroupsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: DeleteConsumerGroupsObserverError) -> KafkaError {
    let public = match error {
        DeleteConsumerGroupsObserverError::AlreadyObserved => ErrorKind::State,
        DeleteConsumerGroupsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
