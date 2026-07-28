//! Exhaustive stable translation of engine-owned AlterReplicaLogDirs outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{AlterReplicaLogDirsResult, BatchResult, TopicPartitionReplica},
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Batch, BrokerError, DeliveryStatus,
        Failure, FailureKind, ObserverError, Outcome, ReplicaOutcome, ReplicaResult,
    },
    operation::AdminAlterReplicaLogDirsResult,
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
        format!("AlterReplicaLogDirs admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "AlterReplicaLogDirs was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "AlterReplicaLogDirs was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminAlterReplicaLogDirsResult {
    match result {
        Ok(Outcome::Altered(batch)) => Ok(translate_batch(batch)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_batch(batch: Batch) -> AlterReplicaLogDirsResult {
    let (throttle_time_ms, replicas) = batch.into_parts();
    AlterReplicaLogDirsResult::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        BatchResult::new(replicas.into_iter().map(translate_replica).collect()),
    )
}

fn translate_replica(replica: ReplicaOutcome) -> (TopicPartitionReplica, Result<(), KafkaError>) {
    let (broker_id, topic, partition, result) = replica.into_parts();
    let identity = TopicPartitionReplica::new(topic, partition, broker_id);
    let result = match result {
        ReplicaResult::Altered => Ok(()),
        ReplicaResult::BrokerFailed(error) => Err(translate_broker_error(error)),
        ReplicaResult::OperationFailed(failure) => Err(translate_failure(failure)),
    };
    (identity, result)
}

fn translate_broker_error(error: BrokerError) -> KafkaError {
    translate_broker_code(error.code())
}

pub(super) fn translate_broker_code(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned AlterReplicaLogDirs replica broker code {code}"),
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
    KafkaError::new(public, format!("AlterReplicaLogDirs failed: {kind:?}"))
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
