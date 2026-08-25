//! Exhaustive stable translation of concrete engine Admin `ListOffsets` outcomes.

use std::time::Duration;

use kafka_client_engine::{
    AdminListOffsetDescription as EngineDescription,
    AdminListOffsetEngineBrokerError as EngineBrokerError, AdminListOffsetsAcceptedFaultKind,
    AdminListOffsetsAdmissionError, AdminListOffsetsAdmissionErrorKind,
    AdminListOffsetsDeliveryStatus, AdminListOffsetsFailure, AdminListOffsetsFailureKind,
    AdminListOffsetsObserverError, AdminListOffsetsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, ListOffsetsResult, ListOffsetsResultInfo},
};

use super::operation::AdminListOffsetsResult;

pub(super) fn translate_admission_error(error: AdminListOffsetsAdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdminListOffsetsAdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdminListOffsetsAdmissionErrorKind::InvalidRequest
        | AdminListOffsetsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        AdminListOffsetsAdmissionErrorKind::Contended
        | AdminListOffsetsAdmissionErrorKind::Capacity
        | AdminListOffsetsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdminListOffsetsAdmissionErrorKind::Closed => ErrorKind::State,
        AdminListOffsetsAdmissionErrorKind::IdentityExhausted
        | AdminListOffsetsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    let error = KafkaError::new(public, format!("ListOffsets admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent);
    match kind {
        AdminListOffsetsAdmissionErrorKind::Contended
        | AdminListOffsetsAdmissionErrorKind::Capacity
        | AdminListOffsetsAdmissionErrorKind::RetainedBytes => error.with_safe_retry(),
        AdminListOffsetsAdmissionErrorKind::InvalidRequest
        | AdminListOffsetsAdmissionErrorKind::InvalidDeadline
        | AdminListOffsetsAdmissionErrorKind::Closed
        | AdminListOffsetsAdmissionErrorKind::IdentityExhausted
        | AdminListOffsetsAdmissionErrorKind::HostUnavailable => error,
    }
}

pub(super) fn translate_accepted_fault(fault: AdminListOffsetsAcceptedFaultKind) -> KafkaError {
    match fault {
        AdminListOffsetsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ListOffsets was accepted but its host wake failed",
        ),
        AdminListOffsetsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ListOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<AdminListOffsetsOutcome, AdminListOffsetsObserverError>,
) -> AdminListOffsetsResult {
    match result {
        Ok(AdminListOffsetsOutcome::Offsets(batch)) => {
            let (throttle_time_ms, offsets) = batch.into_parts();
            let entries = offsets
                .into_iter()
                .map(|offset| {
                    let (topic, partition, result) = offset.into_parts();
                    (
                        TopicPartition::new(topic, partition),
                        result
                            .map(translate_offset)
                            .map_err(translate_partition_error),
                    )
                })
                .collect();
            Ok(ListOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(AdminListOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_offset(offset: EngineDescription) -> ListOffsetsResultInfo {
    let (offset, timestamp_ms, leader_epoch) = offset.into_parts();
    ListOffsetsResultInfo::new(offset, timestamp_ms, leader_epoch)
}

fn translate_partition_error(error: EngineBrokerError) -> KafkaError {
    let code = error.code();
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned ListOffsets partition broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: AdminListOffsetsFailure) -> KafkaError {
    let kind = failure.kind();
    let public = match kind {
        AdminListOffsetsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        AdminListOffsetsFailureKind::DriverRejected
        | AdminListOffsetsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        AdminListOffsetsFailureKind::Transport => ErrorKind::Transport,
        AdminListOffsetsFailureKind::Compatibility => ErrorKind::Compatibility,
        AdminListOffsetsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("ListOffsets failed: {kind:?}"))
        .with_delivery_status(translate_delivery(failure.delivery()))
}

const fn translate_delivery(delivery: AdminListOffsetsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        AdminListOffsetsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        AdminListOffsetsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: AdminListOffsetsObserverError) -> KafkaError {
    let public = match error {
        AdminListOffsetsObserverError::AlreadyObserved => ErrorKind::State,
        AdminListOffsetsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
