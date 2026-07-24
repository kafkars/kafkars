//! Stable `DescribeTopics` translation scenarios.

use kafka_client_engine::{
    DescribeTopicsAcceptedFaultKind, DescribeTopicsAdmissionErrorKind,
    DescribeTopicsDeliveryStatus, DescribeTopicsFailureKind, DescribeTopicsObserverError,
};

use super::admin_topics_result::{
    partition_error, translate_accepted_fault, translate_admission_kind, translate_failure_parts,
    translate_observer_error,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn distinct_admission_observer_and_fault_categories_remain_distinct() {
    assert_eq!(
        translate_admission_kind(DescribeTopicsAdmissionErrorKind::InvalidRequest).kind(),
        ErrorKind::Configuration
    );
    assert_eq!(
        translate_observer_error(DescribeTopicsObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(DescribeTopicsObserverError::Stale).kind(),
        ErrorKind::Internal
    );
    for fault in [
        DescribeTopicsAcceptedFaultKind::Wake,
        DescribeTopicsAcceptedFaultKind::HostInvariant,
    ] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
}

#[test]
fn top_level_and_partition_codes_remain_exact() {
    let top = translate_failure_parts(
        DescribeTopicsFailureKind::Broker(-32_000),
        DescribeTopicsDeliveryStatus::PossiblySent,
    );
    assert_eq!(top.kind(), ErrorKind::Broker);
    assert_eq!(top.broker_code(), Some(-32_000));
    assert_eq!(top.delivery_status(), Some(DeliveryStatus::PossiblySent));
    let partition = partition_error(-31_999);
    assert_eq!(partition.kind(), ErrorKind::Broker);
    assert_eq!(partition.broker_code(), Some(-31_999));

    let deadline = translate_failure_parts(
        DescribeTopicsFailureKind::DeadlineElapsed,
        DescribeTopicsDeliveryStatus::PossiblySent,
    );
    assert_eq!(deadline.kind(), ErrorKind::Timeout);
    assert_eq!(
        deadline.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );

    let oversized = translate_failure_parts(
        DescribeTopicsFailureKind::ResponseTooLarge,
        DescribeTopicsDeliveryStatus::PossiblySent,
    );
    assert_eq!(oversized.kind(), ErrorKind::Backpressure);
    assert_eq!(
        oversized.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
}

#[test]
fn old_broker_policy_failure_maps_to_public_compatibility() {
    let compatibility = translate_failure_parts(
        DescribeTopicsFailureKind::Compatibility,
        DescribeTopicsDeliveryStatus::NotSent,
    );
    assert_eq!(compatibility.kind(), ErrorKind::Compatibility);
    assert_eq!(
        compatibility.delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
}
