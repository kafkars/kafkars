//! Scenarios for exhaustive producer admission and accepted-fault translation.

use kafka_client_engine::{
    ProducerAcceptedFault as EngineAcceptedFault,
    ProducerAcceptedFaultKind as EngineAcceptedFaultKind,
    ProducerSendCaptureErrorKind as EngineCaptureErrorKind,
    ProducerTrySendError as EngineTrySendError, ProducerTrySendErrorKind as EngineTrySendErrorKind,
};

use super::admission::{
    ProducerAdmissionRejection, accepted_fault_error, accepted_fault_kind, admission_error,
    admission_kind, capture_error_kind, translate_accepted_fault, translate_admission_error,
};
use crate::{DeliveryStatus, ErrorKind, KafkaError, Record, RetryAdvice};

#[test]
fn future_admission_bridge_surface_remains_type_checked() {
    let _ = translate_admission_error as fn(EngineTrySendError) -> ProducerAdmissionRejection;
    let _ = ProducerAdmissionRejection::into_parts
        as fn(ProducerAdmissionRejection) -> (Record, KafkaError);
    let _ = translate_accepted_fault as fn(&EngineAcceptedFault) -> KafkaError;
}

#[test]
fn every_engine_admission_kind_has_one_stable_facade_category() {
    let cases = [
        (EngineTrySendErrorKind::EmptyTopic, ErrorKind::InvalidRecord),
        (
            EngineTrySendErrorKind::MissingExplicitPartition,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::NegativeExplicitPartition,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::DeadlineUnrepresentable,
            ErrorKind::Timeout,
        ),
        (
            EngineTrySendErrorKind::TimestampUnrepresentable,
            ErrorKind::Internal,
        ),
        (EngineTrySendErrorKind::Contended, ErrorKind::Backpressure),
        (
            EngineTrySendErrorKind::CompletionCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::RecordCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::ByteCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::RecordSizeUnrepresentable,
            ErrorKind::InvalidRecord,
        ),
        (
            EngineTrySendErrorKind::BatchCapacity,
            ErrorKind::Backpressure,
        ),
        (
            EngineTrySendErrorKind::AccumulatorPending,
            ErrorKind::Backpressure,
        ),
        (EngineTrySendErrorKind::DeadlineElapsed, ErrorKind::Timeout),
        (EngineTrySendErrorKind::Closed, ErrorKind::State),
        (
            EngineTrySendErrorKind::LocalIdentityExhausted,
            ErrorKind::Internal,
        ),
        (EngineTrySendErrorKind::HostPoisoned, ErrorKind::Internal),
        (
            EngineTrySendErrorKind::InternalInvariant,
            ErrorKind::Internal,
        ),
    ];

    for (engine, facade) in cases {
        assert_eq!(admission_kind(engine), facade);
    }
}

#[test]
fn every_boundary_capture_failure_maps_to_the_matching_admission_failure() {
    assert_eq!(
        capture_error_kind(EngineCaptureErrorKind::DeadlineUnrepresentable),
        EngineTrySendErrorKind::DeadlineUnrepresentable
    );
    assert_eq!(
        capture_error_kind(EngineCaptureErrorKind::TimestampUnrepresentable),
        EngineTrySendErrorKind::TimestampUnrepresentable
    );
}

#[test]
fn pre_ownership_admission_failure_is_exactly_not_sent() {
    let error = admission_error(
        EngineTrySendErrorKind::InternalInvariant,
        Some("completion generation disagreed"),
    );

    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert_eq!(error.to_string(), "completion generation disagreed");
    assert_eq!(error.broker_code(), None);
    assert!(error.is_fatal());
    assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry);
}

#[test]
fn admission_advice_is_safe_only_for_bounded_transient_rejection() {
    for kind in [
        EngineTrySendErrorKind::Contended,
        EngineTrySendErrorKind::CompletionCapacity,
        EngineTrySendErrorKind::RecordCapacity,
        EngineTrySendErrorKind::ByteCapacity,
        EngineTrySendErrorKind::BatchCapacity,
        EngineTrySendErrorKind::AccumulatorPending,
    ] {
        let error = admission_error(kind, None);
        assert!(error.is_retriable(), "{kind:?}");
        assert!(!error.is_fatal(), "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::RetrySafe, "{kind:?}");
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }

    for kind in [
        EngineTrySendErrorKind::EmptyTopic,
        EngineTrySendErrorKind::DeadlineElapsed,
        EngineTrySendErrorKind::Closed,
    ] {
        let error = admission_error(kind, None);
        assert!(!error.is_retriable(), "{kind:?}");
        assert!(!error.is_fatal(), "{kind:?}");
        assert_eq!(error.retry_advice(), RetryAdvice::DoNotRetry, "{kind:?}");
    }
}

#[test]
fn accepted_execution_faults_remain_internal_without_invented_delivery_status() {
    let invariant = accepted_fault_error(
        EngineAcceptedFaultKind::HostInvariant,
        "post-ownership invariant",
    );
    assert_eq!(
        accepted_fault_kind(EngineAcceptedFaultKind::HostInvariant),
        ErrorKind::Internal
    );
    assert_eq!(invariant.delivery_status(), None);
    assert!(invariant.is_fatal());
    assert_eq!(invariant.retry_advice(), RetryAdvice::DoNotRetry);

    let wake = accepted_fault_error(EngineAcceptedFaultKind::Wake, "advisory wake failure");
    assert_eq!(
        accepted_fault_kind(EngineAcceptedFaultKind::Wake),
        ErrorKind::Internal
    );
    assert_eq!(wake.delivery_status(), None);
    assert!(!wake.is_fatal());
    assert!(!wake.is_retriable());
    assert_eq!(wake.retry_advice(), RetryAdvice::DoNotRetry);
}
