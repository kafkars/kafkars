//! Stable `DescribeConfigs` translation scenarios.

use kafka_client_engine::{
    DescribeConfigsAcceptedFaultKind, DescribeConfigsAdmissionErrorKind,
    DescribeConfigsDeliveryStatus, DescribeConfigsFailureKind, DescribeConfigsObserverError,
};

use super::admin_configs_result::{
    translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error, translate_resource_error_parts,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn only_unadmitted_config_contention_is_safe_to_reconstruct() {
    use DescribeConfigsAdmissionErrorKind as Admission;

    for kind in [
        Admission::InvalidRequest,
        Admission::UnsupportedResource,
        Admission::InvalidDeadline,
        Admission::Contended,
        Admission::Capacity,
        Admission::RetainedBytes,
        Admission::Closed,
        Admission::IdentityExhausted,
        Admission::HostUnavailable,
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(
            error.retry_advice(),
            if kind == Admission::Contended {
                RetryAdvice::RetrySafe
            } else {
                RetryAdvice::DoNotRetry
            },
            "admission category {kind:?}"
        );
    }
}

#[test]
fn all_admission_observer_and_fault_categories_remain_stable() {
    assert_eq!(
        translate_admission_kind(DescribeConfigsAdmissionErrorKind::InvalidRequest).kind(),
        ErrorKind::Configuration
    );
    assert_eq!(
        translate_admission_kind(DescribeConfigsAdmissionErrorKind::UnsupportedResource).kind(),
        ErrorKind::Configuration
    );
    assert_eq!(
        translate_observer_error(DescribeConfigsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(DescribeConfigsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        DescribeConfigsAcceptedFaultKind::Wake,
        DescribeConfigsAcceptedFaultKind::HostInvariant,
    ] {
        let error = translate_accepted_fault(fault);
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
    }
}

#[test]
fn resource_diagnostic_code_delivery_and_truncation_remain_exact() {
    let error = translate_resource_error_parts(-32_123, Some("future broker"), true);
    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-32_123));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.diagnostic_truncated());
}

#[test]
fn failures_preserve_category_and_driver_authoritative_delivery() {
    let cases = [
        (
            DescribeConfigsFailureKind::DeadlineElapsed,
            ErrorKind::Timeout,
        ),
        (
            DescribeConfigsFailureKind::DriverRejected,
            ErrorKind::Backpressure,
        ),
        (
            DescribeConfigsFailureKind::ResponseTooLarge,
            ErrorKind::Backpressure,
        ),
        (DescribeConfigsFailureKind::Transport, ErrorKind::Transport),
        (
            DescribeConfigsFailureKind::InvalidResponse,
            ErrorKind::Broker,
        ),
        (
            DescribeConfigsFailureKind::Compatibility,
            ErrorKind::Compatibility,
        ),
    ];
    for (failure, expected) in cases {
        let error = translate_failure_parts(failure, DescribeConfigsDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
    }
}
