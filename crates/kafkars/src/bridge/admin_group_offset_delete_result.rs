//! Exhaustive stable translation of concrete engine group-offset deletion outcomes.

use std::time::Duration;

use kafka_client_engine::{
    DeleteConsumerGroupOffsetBrokerError as EngineBrokerError,
    DeleteConsumerGroupOffsetsAcceptedFaultKind, DeleteConsumerGroupOffsetsAdmissionError,
    DeleteConsumerGroupOffsetsAdmissionErrorKind, DeleteConsumerGroupOffsetsDeliveryStatus,
    DeleteConsumerGroupOffsetsFailure, DeleteConsumerGroupOffsetsFailureKind,
    DeleteConsumerGroupOffsetsObserverError, DeleteConsumerGroupOffsetsOutcome,
};

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition,
    admin::{BatchResult, DeleteConsumerGroupOffsetsResult},
    bridge::admin_group_offset_delete_operation::AdminDeleteConsumerGroupOffsetsResult,
};

pub(super) fn translate_admission_error(
    error: DeleteConsumerGroupOffsetsAdmissionError,
) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(
    kind: DeleteConsumerGroupOffsetsAdmissionErrorKind,
) -> KafkaError {
    let public = match kind {
        DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest
        | DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline => ErrorKind::Configuration,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::Contended
        | DeleteConsumerGroupOffsetsAdmissionErrorKind::Capacity
        | DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::Closed => ErrorKind::State,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted
        | DeleteConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
    };
    KafkaError::new(
        public,
        format!("DeleteConsumerGroupOffsets admission failed: {kind:?}"),
    )
    .with_delivery_status(DeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(
    fault: DeleteConsumerGroupOffsetsAcceptedFaultKind,
) -> KafkaError {
    match fault {
        DeleteConsumerGroupOffsetsAcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DeleteConsumerGroupOffsets was accepted but its host wake failed",
        ),
        DeleteConsumerGroupOffsetsAcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DeleteConsumerGroupOffsets was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<DeleteConsumerGroupOffsetsOutcome, DeleteConsumerGroupOffsetsObserverError>,
) -> AdminDeleteConsumerGroupOffsetsResult {
    match result {
        Ok(DeleteConsumerGroupOffsetsOutcome::Deleted(batch)) => {
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
            Ok(DeleteConsumerGroupOffsetsResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                BatchResult::new(entries),
            ))
        }
        Ok(DeleteConsumerGroupOffsetsOutcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_partition_error(error: EngineBrokerError) -> KafkaError {
    partition_error(error.code())
}

pub(super) fn partition_error(code: i16) -> KafkaError {
    KafkaError::new(
        ErrorKind::Broker,
        format!("Kafka returned OffsetDelete partition broker code {code}"),
    )
    .with_broker_code(Some(code))
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

fn translate_failure(failure: DeleteConsumerGroupOffsetsFailure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(
    kind: DeleteConsumerGroupOffsetsFailureKind,
    delivery: DeleteConsumerGroupOffsetsDeliveryStatus,
) -> KafkaError {
    let (public, broker_code) = match kind {
        DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed => (ErrorKind::Timeout, None),
        DeleteConsumerGroupOffsetsFailureKind::DriverRejected
        | DeleteConsumerGroupOffsetsFailureKind::ResponseTooLarge => {
            (ErrorKind::Backpressure, None)
        }
        DeleteConsumerGroupOffsetsFailureKind::Transport => (ErrorKind::Transport, None),
        DeleteConsumerGroupOffsetsFailureKind::Broker(code) => (ErrorKind::Broker, Some(code)),
        DeleteConsumerGroupOffsetsFailureKind::Compatibility => (ErrorKind::Compatibility, None),
        DeleteConsumerGroupOffsetsFailureKind::InvalidResponse => (ErrorKind::Broker, None),
    };
    KafkaError::new(
        public,
        format!("DeleteConsumerGroupOffsets failed: {kind:?}"),
    )
    .with_broker_code(broker_code)
    .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeleteConsumerGroupOffsetsDeliveryStatus) -> DeliveryStatus {
    match delivery {
        DeleteConsumerGroupOffsetsDeliveryStatus::NotSent => DeliveryStatus::NotSent,
        DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent => DeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(
    error: DeleteConsumerGroupOffsetsObserverError,
) -> KafkaError {
    let public = match error {
        DeleteConsumerGroupOffsetsObserverError::AlreadyObserved => ErrorKind::State,
        DeleteConsumerGroupOffsetsObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
