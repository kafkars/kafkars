//! Stable group-offset deletion error translation scenarios.

use kafka_client_engine::{
    DeleteConsumerGroupOffsetsAcceptedFaultKind, DeleteConsumerGroupOffsetsAdmissionErrorKind,
    DeleteConsumerGroupOffsetsDeliveryStatus, DeleteConsumerGroupOffsetsFailureKind,
    DeleteConsumerGroupOffsetsObserverError,
};

use crate::{DeliveryStatus, ErrorKind};

use super::admin_group_offset_delete_result::{
    partition_error, translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error,
};

#[test]
fn admission_observer_and_accepted_fault_categories_remain_distinct() {
    assert_eq!(
        translate_admission_kind(DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest)
            .kind(),
        ErrorKind::Configuration
    );
    assert_eq!(
        translate_observer_error(DeleteConsumerGroupOffsetsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(DeleteConsumerGroupOffsetsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        DeleteConsumerGroupOffsetsAcceptedFaultKind::Wake,
        DeleteConsumerGroupOffsetsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn whole_and_partition_broker_codes_remain_exact_signed_and_possibly_sent() {
    let whole = translate_failure_parts(
        DeleteConsumerGroupOffsetsFailureKind::Broker(-32_000),
        DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    );
    assert_eq!(whole.kind(), ErrorKind::Broker);
    assert_eq!(whole.broker_code(), Some(-32_000));
    assert_eq!(whole.delivery_status(), Some(DeliveryStatus::PossiblySent));

    let partition = partition_error(-31_999);
    assert_eq!(partition.kind(), ErrorKind::Broker);
    assert_eq!(partition.broker_code(), Some(-31_999));
    assert_eq!(
        partition.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
}

#[test]
fn deadline_capacity_compatibility_and_delivery_remain_exhaustive() {
    let deadline = translate_failure_parts(
        DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
    );
    assert_eq!(deadline.kind(), ErrorKind::Timeout);
    assert_eq!(deadline.delivery_status(), Some(DeliveryStatus::NotSent));

    let oversized = translate_failure_parts(
        DeleteConsumerGroupOffsetsFailureKind::ResponseTooLarge,
        DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    );
    assert_eq!(oversized.kind(), ErrorKind::Backpressure);

    let compatibility = translate_failure_parts(
        DeleteConsumerGroupOffsetsFailureKind::Compatibility,
        DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
    );
    assert_eq!(compatibility.kind(), ErrorKind::Compatibility);
}
