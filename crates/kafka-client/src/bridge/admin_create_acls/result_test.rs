//! Public ACL value, error category, and delivery translation tests.

use crate::{DeliveryStatus, ErrorKind, admin::CreateAclResult};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        PreparedCreateAclsOutcomes, translate_accepted_fault, translate_admission_kind,
        translate_binding_parts, translate_broker_parts, translate_failure_parts,
        translate_observer_error,
    },
};

#[test]
fn public_outcome_capacity_is_fallibly_prepared_before_admission() {
    assert!(PreparedCreateAclsOutcomes::try_new(2).is_ok());
    assert!(PreparedCreateAclsOutcomes::try_new(usize::MAX).is_err());
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
fn exact_acl_codes_and_nullable_broker_diagnostics_are_preserved() {
    let binding = translate_binding_parts(
        101,
        "orders".to_owned(),
        102,
        "User:alice".to_owned(),
        "*".to_owned(),
        103,
        104,
    );
    assert_eq!(binding.pattern().resource_type().code(), 101);
    assert_eq!(binding.pattern().pattern_type().code(), 102);
    assert_eq!(binding.entry().operation().code(), 103);
    assert_eq!(binding.entry().permission_type().code(), 104);

    let absent = translate_broker_parts(-731, None, false);
    assert_eq!(absent.code(), -731);
    assert_eq!(absent.message(), None);
    assert!(!absent.message_truncated());

    let present = translate_broker_parts(731, Some("denied".to_owned()), true);
    let result = CreateAclResult::BrokerFailed(present);
    let CreateAclResult::BrokerFailed(error) = result else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), 731);
    assert_eq!(error.message(), Some("denied"));
    assert!(error.message_truncated());
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
