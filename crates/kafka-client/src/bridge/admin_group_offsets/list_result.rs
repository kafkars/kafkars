//! Exhaustive stable translation of concrete engine group-offset outcomes.

use std::time::Duration;

use kafka_client_engine::{
    GroupOffsetBrokerError as EngineBrokerError, GroupOffsetDescription as EngineOffsetDescription,
    ListConsumerGroupOffsetsAcceptedFaultKind, ListConsumerGroupOffsetsAdmissionError,
    ListConsumerGroupOffsetsAdmissionErrorKind, ListConsumerGroupOffsetsDeliveryStatus,
    ListConsumerGroupOffsetsFailure, ListConsumerGroupOffsetsFailureKind,
    ListConsumerGroupOffsetsObserverError, ListConsumerGroupOffsetsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, ConsumerGroupOffset, ListConsumerGroupOffsetsResult},
};

use super::list_operation::AdminListConsumerGroupOffsetsResult;

pub(super) fn translate_admission_error(
    error: ListConsumerGroupOffsetsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(
    kind: ListConsumerGroupOffsetsAdmissionErrorKind,
) -> KafkaError {
    let public = match kind {
        ListConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest
        | ListConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        ListConsumerGroupOffsetsAdmissionErrorKind::Contended
        | ListConsumerGroupOffsetsAdmissionErrorKind::Capacity
        | ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        ListConsumerGroupOffsetsAdmissionErrorKind::Closed => ErrorKind::State,
        ListConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted
        | ListConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("ListConsumerGroupOffsets admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: ListConsumerGroupOffsetsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        ListConsumerGroupOffsetsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "ListConsumerGroupOffsets was accepted but its host wake failed",
        ),
        ListConsumerGroupOffsetsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "ListConsumerGroupOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<ListConsumerGroupOffsetsOutcome, ListConsumerGroupOffsetsObserverError>,
) -> AdminListConsumerGroupOffsetsResult {
    match result {
        Ok(ListConsumerGroupOffsetsOutcome::Offsets(batch)) => {
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
            Ok(ListConsumerGroupOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(ListConsumerGroupOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_offset(offset: EngineOffsetDescription) -> ConsumerGroupOffset {
    let (committed_offset, leader_epoch, metadata) = offset.into_parts();
    ConsumerGroupOffset::new(committed_offset, leader_epoch, metadata)
}

fn translate_partition_error(error: EngineBrokerError) -> KafkaError {
    partition_error(error.code())
}

pub(super) fn partition_error(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned OffsetFetch partition broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: ListConsumerGroupOffsetsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: ListConsumerGroupOffsetsFailureKind,
    delivery: ListConsumerGroupOffsetsDeliveryStatus,
) -> KafkaError {
    let (public, broker_code) = match kind {
        ListConsumerGroupOffsetsFailureKind::DeadlineElapsed => (ErrorKind::Timeout, None),
        ListConsumerGroupOffsetsFailureKind::DriverRejected
        | ListConsumerGroupOffsetsFailureKind::ResponseTooLarge => (ErrorKind::Backpressure, None),
        ListConsumerGroupOffsetsFailureKind::Transport => (ErrorKind::Transport, None),
        ListConsumerGroupOffsetsFailureKind::Broker(code) => (ErrorKind::Broker, Some(code)),
        ListConsumerGroupOffsetsFailureKind::Compatibility => (ErrorKind::Compatibility, None),
        ListConsumerGroupOffsetsFailureKind::InvalidResponse => (ErrorKind::Broker, None),
    };
    KafkaError::new(public, format!("ListConsumerGroupOffsets failed: {kind:?}"))
        .with_broker_code(broker_code)
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: ListConsumerGroupOffsetsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        ListConsumerGroupOffsetsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        ListConsumerGroupOffsetsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ListConsumerGroupOffsetsObserverError) -> KafkaError {
    let public = match error {
        ListConsumerGroupOffsetsObserverError::AlreadyObserved => ErrorKind::State,
        ListConsumerGroupOffsetsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
