//! Public filter values, failure categories, and delivery translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        PreparedDeleteAclsOutcomes, translate_accepted_fault, translate_admission_kind,
        translate_failure_parts, translate_observer_error,
    },
    value::{translate_broker_parts, translate_filter_parts},
};

#[test]
fn outer_public_storage_is_fallibly_prepared_before_admission() {
    assert!(PreparedDeleteAclsOutcomes::try_new(2).is_ok());
    assert!(PreparedDeleteAclsOutcomes::try_new(usize::MAX).is_err());
}

#[test]
fn admission_categories_remain_exhaustive_and_definitely_unsent() {
    for (kind, expected) in [
        (AdmissionErrorKind::InvalidRequest, ErrorKind::Configuration),
        (
            AdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (AdmissionErrorKind::Contended, ErrorKind::Backpressure),
        (AdmissionErrorKind::Capacity, ErrorKind::Backpressure),
        (AdmissionErrorKind::RetainedBytes, ErrorKind::Backpressure),
        (AdmissionErrorKind::Closed, ErrorKind::State),
        (AdmissionErrorKind::IdentityExhausted, ErrorKind::Internal),
        (AdmissionErrorKind::HostUnavailable, ErrorKind::Internal),
    ] {
        let error = translate_admission_kind(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn accepted_faults_remain_advisory_internal_diagnostics() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn mechanism_failures_preserve_category_and_delivery_certainty() {
    for (kind, expected) in [
        (FailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (FailureKind::DriverRejected, ErrorKind::Backpressure),
        (FailureKind::Transport, ErrorKind::Transport),
        (FailureKind::ResponseTooLarge, ErrorKind::Backpressure),
        (FailureKind::Compatibility, ErrorKind::Compatibility),
        (FailureKind::InvalidResponse, ErrorKind::Broker),
    ] {
        let error = translate_failure_parts(kind, EngineDeliveryStatus::PossiblySent);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    }
}

#[test]
fn exact_filter_codes_and_nullable_selectors_are_preserved() {
    let filter = translate_filter_parts(
        101,
        Some("orders".to_owned()),
        102,
        Some("User:alice".to_owned()),
        None,
        103,
        104,
    );

    assert_eq!(filter.resource_type().code(), 101);
    assert_eq!(filter.resource_name(), Some("orders"));
    assert_eq!(filter.pattern_type().code(), 102);
    assert_eq!(filter.principal(), Some("User:alice"));
    assert_eq!(filter.host(), None);
    assert_eq!(filter.operation().code(), 103);
    assert_eq!(filter.permission_type().code(), 104);

    let absent = translate_broker_parts(-731, None, false);
    assert_eq!(absent.code(), -731);
    assert_eq!(absent.message(), None);
    assert!(!absent.message_truncated());

    let present = translate_broker_parts(731, Some("denied".to_owned()), true);
    assert_eq!(present.code(), 731);
    assert_eq!(present.message(), Some("denied"));
    assert!(present.message_truncated());
}

#[test]
fn observer_failures_keep_state_and_internal_categories() {
    assert_eq!(
        translate_observer_error(ObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ObserverError::Stale).kind(),
        ErrorKind::Internal
    );
}
