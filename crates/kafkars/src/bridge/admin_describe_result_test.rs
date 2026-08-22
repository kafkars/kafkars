//! Exhaustive stable `DescribeCluster` translation scenarios.

use kafka_client_engine::{
    DescribeClusterAcceptedFaultKind, DescribeClusterAdmissionErrorKind,
    DescribeClusterDeliveryStatus, DescribeClusterFailureKind, DescribeClusterObserverError,
};

use super::admin_describe_result::{
    translate_accepted_fault, translate_admission_kind, translate_broker_error_parts,
    translate_description_parts, translate_failure_parts, translate_observer_error,
};
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

#[test]
fn every_admission_category_preserves_pre_admission_retry_safety() {
    let cases = [
        (
            DescribeClusterAdmissionErrorKind::InvalidDeadline,
            ErrorKind::Configuration,
        ),
        (
            DescribeClusterAdmissionErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (DescribeClusterAdmissionErrorKind::Closed, ErrorKind::State),
        (
            DescribeClusterAdmissionErrorKind::Capacity,
            ErrorKind::Backpressure,
        ),
        (
            DescribeClusterAdmissionErrorKind::RetainedBytes,
            ErrorKind::Backpressure,
        ),
        (
            DescribeClusterAdmissionErrorKind::IdentityExhausted,
            ErrorKind::Internal,
        ),
        (
            DescribeClusterAdmissionErrorKind::HostUnavailable,
            ErrorKind::Internal,
        ),
    ];
    for (engine, public) in cases {
        let error = translate_admission_kind(engine);
        assert_eq!(error.kind(), public);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        let expected = match engine {
            DescribeClusterAdmissionErrorKind::Contended
            | DescribeClusterAdmissionErrorKind::Capacity
            | DescribeClusterAdmissionErrorKind::RetainedBytes => RetryAdvice::RetrySafe,
            DescribeClusterAdmissionErrorKind::InvalidDeadline
            | DescribeClusterAdmissionErrorKind::Closed
            | DescribeClusterAdmissionErrorKind::IdentityExhausted
            | DescribeClusterAdmissionErrorKind::HostUnavailable => RetryAdvice::DoNotRetry,
        };
        assert_eq!(error.retry_advice(), expected);
    }
}

#[test]
fn controller_identity_crosses_the_bridge_without_becoming_policy() {
    let description =
        translate_description_parts(String::from("cluster-a"), Some(17), Vec::new(), Some(0x21));
    assert_eq!(description.cluster_id(), "cluster-a");
    assert_eq!(description.controller_id(), Some(17));
    assert_eq!(description.authorized_operations(), Some(0x21));
}

#[test]
fn signed_code_nullable_diagnostic_and_certainty_are_lossless() {
    let broker = translate_broker_error_parts(-32_000, None, false);
    assert_eq!(broker.broker_code(), Some(-32_000));
    assert_eq!(broker.delivery_status(), Some(DeliveryStatus::PossiblySent));
    let failure = translate_failure_parts(
        DescribeClusterFailureKind::Transport,
        DescribeClusterDeliveryStatus::PossiblySent,
    );
    assert_eq!(failure.kind(), ErrorKind::Transport);
    assert_eq!(
        failure.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
}

#[test]
fn authentication_rejection_maps_to_public_access() {
    let failure = translate_failure_parts(
        DescribeClusterFailureKind::Authentication,
        DescribeClusterDeliveryStatus::NotSent,
    );
    assert_eq!(failure.kind(), ErrorKind::Access);
    assert_eq!(failure.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn protocol_incompatibility_remains_definitely_unsent() {
    let failure = translate_failure_parts(
        DescribeClusterFailureKind::Compatibility,
        DescribeClusterDeliveryStatus::NotSent,
    );
    assert_eq!(failure.kind(), ErrorKind::Compatibility);
    assert_eq!(failure.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn observer_and_accepted_fault_categories_remain_distinct() {
    assert_eq!(
        translate_observer_error(DescribeClusterObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(DescribeClusterObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        DescribeClusterAcceptedFaultKind::Wake,
        DescribeClusterAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}
