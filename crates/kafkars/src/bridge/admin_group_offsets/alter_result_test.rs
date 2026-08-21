//! Stable group-offset alteration error translation scenarios.

use kafka_client_engine::{
    AlterConsumerGroupOffsetsAcceptedFaultKind, AlterConsumerGroupOffsetsAdmissionErrorKind,
    AlterConsumerGroupOffsetsDeliveryStatus, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsObserverError,
};

use crate::{DeliveryStatus, ErrorKind};

use super::alter_result::{
    partition_error, translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error,
};

#[test]
fn every_admission_observer_and_accepted_fault_category_is_stable() {
    let admissions = [
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::Closed,
            ErrorKind::State,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            AlterConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (kind, expected) in admissions {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
    assert_eq!(
        translate_observer_error(AlterConsumerGroupOffsetsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(AlterConsumerGroupOffsetsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        AlterConsumerGroupOffsetsAcceptedFaultKind::Wake,
        AlterConsumerGroupOffsetsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn whole_failures_and_partition_codes_preserve_delivery_certainty() {
    let cases = [
        (
            AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            AlterConsumerGroupOffsetsFailureKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (
            AlterConsumerGroupOffsetsFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
        ),
        (
            AlterConsumerGroupOffsetsFailureKind::Transport,
            ErrorKind::Transport,
        ),
        (
            AlterConsumerGroupOffsetsFailureKind::Compatibility,
            ErrorKind::Compatibility,
        ),
        (
            AlterConsumerGroupOffsetsFailureKind::InvalidResponse,
            ErrorKind::Broker,
        ),
    ];
    for (kind, expected) in cases {
        let error =
            translate_failure_parts(kind, AlterConsumerGroupOffsetsDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
    let partition = partition_error(-32_000);
    assert_eq!(partition.broker_code(), Some(-32_000));
    assert_eq!(
        partition.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
}
