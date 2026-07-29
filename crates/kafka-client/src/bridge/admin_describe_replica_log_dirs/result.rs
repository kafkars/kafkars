//! Exhaustive stable translation of engine-owned DescribeReplicaLogDirs outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        BatchResult, DescribeReplicaLogDirsResult, ReplicaLogDirInfo as PublicInfo,
        ReplicaLogDirLocation as PublicLocation, TopicPartitionReplica,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Failure, FailureKind, Info, Location, ObserverError, Outcome, ReplicaOutcome,
        ReplicaResult,
    },
    operation::AdminDescribeReplicaLogDirsResult,
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
        format!("DescribeReplicaLogDirs admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeReplicaLogDirs was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeReplicaLogDirs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeReplicaLogDirsResult {
    match result {
        Ok(Outcome::Described(batch)) => Ok(translate_batch(batch)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> DescribeReplicaLogDirsResult {
    let (throttle_time_ms, replicas) = batch.into_parts();
    DescribeReplicaLogDirsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(replicas.into_iter().map(translate_replica).collect()),
    )
}

fn translate_replica(
    replica: ReplicaOutcome,
) -> (TopicPartitionReplica, Result<PublicInfo, KafkaError>) {
    let (target, result) = replica.into_parts();
    let identity =
        TopicPartitionReplica::new(target.topic(), target.partition(), target.broker_id());
    let result = match result {
        ReplicaResult::Described(info) => Ok(translate_info(info)),
        ReplicaResult::BrokerFailed(error) => Err(translate_broker_error(error)),
        ReplicaResult::OperationFailed(failure) => Err(translate_failure(failure)),
    };
    (identity, result)
}

fn translate_info(info: Info) -> PublicInfo {
    let (current, future) = info.into_parts();
    PublicInfo::new(
        current.map(translate_location),
        future.map(translate_location),
    )
}

fn translate_location(location: Location) -> PublicLocation {
    let (path, offset_lag) = location.into_parts();
    PublicLocation::new(path, offset_lag)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    translate_broker_code(error.code())
}

pub(super) fn translate_broker_code(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned DescribeReplicaLogDirs replica broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(PublicDeliveryStatus::PossiblySent)
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
        FailureKind::NotAttempted => ErrorKind::State,
    };
    KafkaError::new(public, format!("DescribeReplicaLogDirs failed: {kind:?}"))
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
