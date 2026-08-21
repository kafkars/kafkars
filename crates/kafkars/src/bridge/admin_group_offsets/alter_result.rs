//! Exhaustive stable translation of concrete engine group-offset alteration outcomes.

use std::time::Duration;

use kafka_client_engine::{
    AlterConsumerGroupOffsetBrokerError as EngineBrokerError,
    AlterConsumerGroupOffsetsAcceptedFaultKind, AlterConsumerGroupOffsetsAdmissionError,
    AlterConsumerGroupOffsetsAdmissionErrorKind, AlterConsumerGroupOffsetsDeliveryStatus,
    AlterConsumerGroupOffsetsFailure, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsObserverError, AlterConsumerGroupOffsetsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{AlterConsumerGroupOffsetsResult, BatchResult},
};

use super::alter_operation::AdminAlterConsumerGroupOffsetsResult;

pub(super) fn translate_admission_error(
    error: &AlterConsumerGroupOffsetsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(
    kind: AlterConsumerGroupOffsetsAdmissionErrorKind,
) -> KafkaError {
    let public = match kind {
        AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest
        | AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        AlterConsumerGroupOffsetsAdmissionErrorKind::Contended
        | AlterConsumerGroupOffsetsAdmissionErrorKind::Capacity
        | AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AlterConsumerGroupOffsetsAdmissionErrorKind::Closed => ErrorKind::State,
        AlterConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted
        | AlterConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("AlterConsumerGroupOffsets admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: AlterConsumerGroupOffsetsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        AlterConsumerGroupOffsetsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "AlterConsumerGroupOffsets was accepted but its host wake failed",
        ),
        AlterConsumerGroupOffsetsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "AlterConsumerGroupOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<AlterConsumerGroupOffsetsOutcome, AlterConsumerGroupOffsetsObserverError>,
) -> AdminAlterConsumerGroupOffsetsResult {
    match result {
        Ok(AlterConsumerGroupOffsetsOutcome::Altered(batch)) => {
            let (throttle_time_ms, offsets) = batch.into_parts();
            let entries = offsets
                .into_iter()
                .map(|offset| {
                    let (topic, partition, result) = offset.into_parts();
                    (
                        TopicPartition::new(topic, partition),
                        result.map_err(translate_partition_error),
                    )
                })
                .collect();
            Ok(AlterConsumerGroupOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(AlterConsumerGroupOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_partition_error(error: EngineBrokerError) -> KafkaError {
    partition_error(error.code())
}

pub(super) fn partition_error(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned OffsetCommit partition broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: AlterConsumerGroupOffsetsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: AlterConsumerGroupOffsetsFailureKind,
    delivery: AlterConsumerGroupOffsetsDeliveryStatus,
) -> KafkaError {
    let public = match kind {
        AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed => ErrorKind::Timeout,
        AlterConsumerGroupOffsetsFailureKind::DriverRejected
        | AlterConsumerGroupOffsetsFailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        AlterConsumerGroupOffsetsFailureKind::Transport => ErrorKind::Transport,
        AlterConsumerGroupOffsetsFailureKind::Compatibility => ErrorKind::Compatibility,
        AlterConsumerGroupOffsetsFailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(
        public,
        format!("AlterConsumerGroupOffsets failed: {kind:?}"),
    )
    .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: AlterConsumerGroupOffsetsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        AlterConsumerGroupOffsetsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        AlterConsumerGroupOffsetsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(
    error: AlterConsumerGroupOffsetsObserverError,
) -> KafkaError {
    let public = match error {
        AlterConsumerGroupOffsetsObserverError::AlreadyObserved => ErrorKind::State,
        AlterConsumerGroupOffsetsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
