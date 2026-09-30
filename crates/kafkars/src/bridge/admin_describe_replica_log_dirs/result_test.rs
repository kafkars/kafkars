//! `DescribeReplicaLogDirs` error and public value translation coverage.

use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        translate_accepted_fault, translate_admission_kind, translate_broker_code,
        translate_failure_parts, translate_observer_error,
    },
};

#[test]
fn only_pre_admission_resource_pressure_is_retry_safe() {
    let cases = [
        (AdmissionErrorKind::InvalidRequest, ErrorKind::Configuration),
        (
            AdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (AdmissionErrorKind::Contended, ErrorKind::Backpressure),
        (AdmissionErrorKind::Closed, ErrorKind::State),
        (AdmissionErrorKind::Capacity, ErrorKind::Backpressure),
        (AdmissionErrorKind::RetainedBytes, ErrorKind::Backpressure),
        (AdmissionErrorKind::IdentityExhausted, ErrorKind::Internal),
        (AdmissionErrorKind::HostUnavailable, ErrorKind::Internal),
    ];

    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        let expected = match engine {
            AdmissionErrorKind::Contended
            | AdmissionErrorKind::Capacity
            | AdmissionErrorKind::RetainedBytes => RetryAdvice::RetrySafe,
            AdmissionErrorKind::InvalidRequest
            | AdmissionErrorKind::InvalidDeadline
            | AdmissionErrorKind::Closed
            | AdmissionErrorKind::IdentityExhausted
            | AdmissionErrorKind::HostUnavailable => RetryAdvice::DoNotRetry,
        };
        assert_eq!(error.retry_advice(), expected);
        assert_eq!(error.broker_code(), None);
    }
}

#[test]
fn accepted_failures_preserve_certainty_without_reconstruction_advice() {
    for kind in [
        FailureKind::DeadlineElapsed,
        FailureKind::DriverRejected,
        FailureKind::ResponseTooLarge,
        FailureKind::Transport,
        FailureKind::Compatibility,
        FailureKind::InvalidResponse,
        FailureKind::NotAttempted,
    ] {
        for (engine, public) in [
            (EngineDeliveryStatus::NotSent, DeliveryStatus::NotSent),
            (
                EngineDeliveryStatus::PossiblySent,
                DeliveryStatus::PossiblySent,
            ),
        ] {
            let error = translate_failure_parts(kind, engine);
            assert_eq!(error.delivery_status(), Some(public));
            assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
        }
    }
}

#[test]
fn accepted_faults_and_observer_loss_remain_non_retryable() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        let error = translate_accepted_fault(fault);
        assert_eq!(error.kind(), ErrorKind::Internal);
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
        assert_eq!(error.delivery_status(), None);
    }
    for failure in [ObserverError::AlreadyObserved, ObserverError::Stale] {
        let error = translate_observer_error(failure);
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
        assert_eq!(error.delivery_status(), None);
    }
}

#[test]
fn exact_broker_code_and_delivery_are_preserved() {
    let error = translate_broker_code(-32_000);

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-32_000));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn not_attempted_is_distinct_and_definitely_unsent() {
    let error = translate_failure_parts(FailureKind::NotAttempted, EngineDeliveryStatus::NotSent);

    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn stale_observer_maps_to_internal_state_loss() {
    let error = translate_observer_error(super::engine::ObserverError::Stale);

    assert_eq!(error.kind(), ErrorKind::Internal);
}
