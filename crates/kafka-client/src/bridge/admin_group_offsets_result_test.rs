//! Stable group-offset error translation scenarios.

use kafka_client_engine::{
    ListConsumerGroupOffsetsAcceptedFaultKind, ListConsumerGroupOffsetsAdmissionErrorKind,
    ListConsumerGroupOffsetsDeliveryStatus, ListConsumerGroupOffsetsFailureKind,
    ListConsumerGroupOffsetsObserverError,
};

use super::admin_group_offsets_result::{
    partition_error, translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn admission_observer_and_accepted_fault_categories_remain_distinct() {
    assert_eq!(
        translate_admission_kind(ListConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest).kind(),
        ErrorKind::Configuration
    );
    assert_eq!(
        translate_observer_error(ListConsumerGroupOffsetsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ListConsumerGroupOffsetsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        ListConsumerGroupOffsetsAcceptedFaultKind::Wake,
        ListConsumerGroupOffsetsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn whole_and_partition_broker_codes_remain_exact_and_signed() {
    let whole = translate_failure_parts(
        ListConsumerGroupOffsetsFailureKind::Broker(-32_000),
        ListConsumerGroupOffsetsDeliveryStatus::PossiblySent,
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
fn deadline_capacity_and_compatibility_keep_distinct_public_meaning() {
    let deadline = translate_failure_parts(
        ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        ListConsumerGroupOffsetsDeliveryStatus::NotSent,
    );
    assert_eq!(deadline.kind(), ErrorKind::Timeout);
    assert_eq!(deadline.delivery_status(), Some(DeliveryStatus::NotSent));

    let oversized = translate_failure_parts(
        ListConsumerGroupOffsetsFailureKind::ResponseTooLarge,
        ListConsumerGroupOffsetsDeliveryStatus::PossiblySent,
    );
    assert_eq!(oversized.kind(), ErrorKind::Backpressure);

    let compatibility = translate_failure_parts(
        ListConsumerGroupOffsetsFailureKind::Compatibility,
        ListConsumerGroupOffsetsDeliveryStatus::NotSent,
    );
    assert_eq!(compatibility.kind(), ErrorKind::Compatibility);
}
