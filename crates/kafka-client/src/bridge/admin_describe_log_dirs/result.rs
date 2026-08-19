//! Exhaustive stable translation of engine-owned `DescribeLogDirs` outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        BatchResult, DescribeLogDirsResult, LogDirDescription as PublicDirectoryDescription,
        LogDirReplica,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, BrokerFailure,
        BrokerOutcome, BrokerResult, DeliveryStatus, DirectoryDescription, DirectoryOutcome,
        Failure, FailureKind, ObserverError, Outcome, ReplicaInfo,
    },
    operation::AdminDescribeLogDirsResult,
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
        format!("DescribeLogDirs admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeLogDirs was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeLogDirs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeLogDirsResult {
    match result {
        Ok(Outcome::Described(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> DescribeLogDirsResult {
    let (throttle_time_ms, brokers) = batch.into_parts();
    DescribeLogDirsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(brokers.into_iter().map(translate_broker).collect()),
    )
}

fn translate_broker(
    broker: BrokerOutcome,
) -> (
    i32,
    Result<BatchResult<String, PublicDirectoryDescription>, KafkaError>,
) {
    let (broker_id, result) = broker.into_parts();
    let result = match result {
        BrokerResult::Described(log_dirs) => Ok(BatchResult::new(
            log_dirs.into_iter().map(translate_directory).collect(),
        )),
        BrokerResult::BrokerFailed(error) => Err(translate_broker_error(
            error,
            "broker",
            PublicDeliveryStatus::PossiblySent,
        )),
        BrokerResult::OperationFailed(failure) => Err(translate_broker_failure(failure)),
    };
    (broker_id, result)
}

fn translate_directory(
    directory: DirectoryOutcome,
) -> (String, Result<PublicDirectoryDescription, KafkaError>) {
    let (path, result) = directory.into_parts();
    let result = result.map(translate_description).map_err(|error| {
        translate_broker_error(error, "log directory", PublicDeliveryStatus::PossiblySent)
    });
    (path, result)
}

fn translate_description(description: DirectoryDescription) -> PublicDirectoryDescription {
    let (replicas, total_bytes, usable_bytes, cordoned) = description.into_parts();
    PublicDirectoryDescription::new(
        total_bytes,
        usable_bytes,
        cordoned,
        replicas.into_iter().map(translate_replica).collect(),
    )
}

fn translate_replica(replica: ReplicaInfo) -> LogDirReplica {
    let (topic, partition, size_bytes, offset_lag, future) = replica.into_parts();
    LogDirReplica::new(topic, partition, size_bytes, offset_lag, future)
}

fn translate_broker_error(
    error: BrokerError,
    scope: &str,
    delivery: PublicDeliveryStatus,
) -> KafkaError {
    translate_broker_code(error.code(), scope, delivery)
}

pub(super) fn translate_broker_code(
    code: i16,
    scope: &str,
    delivery: PublicDeliveryStatus,
) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned DescribeLogDirs {scope} broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(delivery)
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

fn translate_broker_failure(failure: BrokerFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
        FailureKind::NotAttempted => ErrorKind::State,
    };
    KafkaError::new(public, format!("DescribeLogDirs failed: {kind:?}"))
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
