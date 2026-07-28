//! Exhaustive stable translation of concrete engine Admin `DeleteRecords` outcomes.

use std::time::Duration;

use kafka_client_engine::{
    DeleteRecordsAcceptedFaultKind, DeleteRecordsAdmissionError, DeleteRecordsAdmissionErrorKind,
    DeleteRecordsDeliveryStatus, DeleteRecordsDescription as EngineDescription,
    DeleteRecordsEngineBrokerError as EngineBrokerError, DeleteRecordsFailure,
    DeleteRecordsFailureKind, DeleteRecordsObserverError, DeleteRecordsOutcome,
    DeleteRecordsRequestTarget,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, DeleteRecordsResult, DeleteRecordsResultInfo},
};

use super::operation::AdminDeleteRecordsResult;

pub(super) fn translate_admission_error(error: DeleteRecordsAdmissionError) -> KafkaError {
    let kind = error.kind();
    let public = match kind {
        DeleteRecordsAdmissionErrorKind::InvalidRequest
        | DeleteRecordsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DeleteRecordsAdmissionErrorKind::Contended
        | DeleteRecordsAdmissionErrorKind::Capacity
        | DeleteRecordsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DeleteRecordsAdmissionErrorKind::Closed => ErrorKind::State,
        DeleteRecordsAdmissionErrorKind::IdentityExhausted
        | DeleteRecordsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(public, format!("DeleteRecords admission failed: {kind:?}"))
        .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: DeleteRecordsAcceptedFaultKind) -> KafkaError {
    match fault {
        DeleteRecordsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteRecords was accepted but its host wake failed",
        ),
        DeleteRecordsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteRecords was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DeleteRecordsOutcome, DeleteRecordsObserverError>,
) -> AdminDeleteRecordsResult {
    match result {
        Ok(DeleteRecordsOutcome::Deleted(batch)) => {
            let (throttle_time_ms, records) = batch.into_parts();
            let entries = records.into_iter().map(translate_record).collect();
            Ok(DeleteRecordsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(DeleteRecordsOutcome::Failed(failure)) => Ok(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_record(
    record: kafka_client_engine::DeleteRecordsEngineResult,
) -> (TopicPartition, Result<DeleteRecordsResultInfo, KafkaError>) {
    let (topic, partition, result) = record.into_parts();
    (
        TopicPartition::new(topic, partition),
        result
            .map(translate_result)
            .map_err(translate_partition_error),
    )
}

fn translate_result(records: EngineDescription) -> DeleteRecordsResultInfo {
    DeleteRecordsResultInfo::new(records.low_watermark())
}

fn translate_partition_error(error: EngineBrokerError) -> KafkaError {
    let code = error.code();
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned DeleteRecords partition broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: DeleteRecordsFailure) -> DeleteRecordsResult {
    let (kind, delivery, throttle_time_ms, completed, failed_target, unattempted) =
        failure.into_parts();
    let completed = completed.into_iter().map(translate_record).collect();
    partial_result(
        throttle_time_ms,
        completed,
        translate_target(failed_target),
        translate_attempt_failure(kind, delivery),
        unattempted.into_iter().map(translate_target).collect(),
    )
}

fn translate_attempt_failure(
    kind: DeleteRecordsFailureKind,
    delivery: DeleteRecordsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        DeleteRecordsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        DeleteRecordsFailureKind::DriverRejected | DeleteRecordsFailureKind::ResponseTooLarge => {
            ErrorKind::Backpressure
        }
        DeleteRecordsFailureKind::Transport => ErrorKind::Transport,
        DeleteRecordsFailureKind::Compatibility => ErrorKind::Compatibility,
        DeleteRecordsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DeleteRecords failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

fn translate_target(target: DeleteRecordsRequestTarget) -> TopicPartition {
    let (topic, partition, _before_offset) = target.into_parts();
    TopicPartition::new(topic, partition)
}

pub(super) fn partial_result(
    throttle_time_ms: u32,
    mut completed: Vec<(TopicPartition, Result<DeleteRecordsResultInfo, KafkaError>)>,
    failed_target: TopicPartition,
    failed_error: KafkaError,
    unattempted: Vec<TopicPartition>,
) -> DeleteRecordsResult {
    completed.reserve(1 + unattempted.len());
    completed.push((failed_target, Err(failed_error)));
    completed.extend(
        unattempted
            .into_iter()
            .map(|target| (target, Err(unattempted_error()))),
    );
    DeleteRecordsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(completed),
    )
}

fn unattempted_error() -> KafkaError {
    KafkaError::new(
        ErrorKind::State,
        "DeleteRecords target was not attempted because an earlier target failed",
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

const fn translate_delivery(delivery: DeleteRecordsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DeleteRecordsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DeleteRecordsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

fn translate_observer_error(error: DeleteRecordsObserverError) -> KafkaError {
    let public = match error {
        DeleteRecordsObserverError::AlreadyObserved => ErrorKind::State,
        DeleteRecordsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
