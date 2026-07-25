//! Stable `IncrementalAlterConfigs` translation scenarios.

use kafka_client_engine::{
    IncrementalAlterConfigsAcceptedFaultKind, IncrementalAlterConfigsAdmissionErrorKind,
    IncrementalAlterConfigsDeliveryStatus, IncrementalAlterConfigsFailureKind,
    IncrementalAlterConfigsObserverError,
};

use super::admin_alter_configs_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_topic_error_parts,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn every_admission_observer_and_accepted_fault_category_is_stable() {
    let admissions = [
        (
            IncrementalAlterConfigsAdmissionErrorKind::InvalidRequest,
            ErrorKind::Configuration,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::Closed,
            ErrorKind::State,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
        (
            IncrementalAlterConfigsAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
    ];
    for (kind, expected) in admissions {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
    assert_eq!(
        translate_observer_error(IncrementalAlterConfigsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(IncrementalAlterConfigsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        IncrementalAlterConfigsAcceptedFaultKind::Wake,
        IncrementalAlterConfigsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn topic_diagnostic_code_delivery_empty_message_and_truncation_remain_exact() {
    let error = translate_topic_error_parts(-32_123, Some(""), true);
    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-32_123));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}

#[test]
fn every_terminal_failure_preserves_category_and_driver_delivery() {
    let cases = [
        (
            IncrementalAlterConfigsFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            IncrementalAlterConfigsFailureKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (
            IncrementalAlterConfigsFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
        ),
        (
            IncrementalAlterConfigsFailureKind::Transport,
            ErrorKind::Transport,
        ),
        (
            IncrementalAlterConfigsFailureKind::InvalidResponse,
            ErrorKind::Broker,
        ),
        (
            IncrementalAlterConfigsFailureKind::Compatibility,
            ErrorKind::Compatibility,
        ),
    ];
    for (failure, expected) in cases {
        let error =
            translate_failure_parts(failure, IncrementalAlterConfigsDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
}
