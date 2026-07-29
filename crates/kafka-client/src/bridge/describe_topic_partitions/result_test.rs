//! API-key 75 admission, failure, broker-code, and delivery translation tests.

use crate::{DeliveryStatus, ErrorKind};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionErrorKind, DeliveryStatus as EngineDeliveryStatus, FailureKind,
        ObserverError,
    },
    result::{
        broker_error, page_from_parts, partition_from_parts, topic_from_parts,
        translate_accepted_fault, translate_admission_kind, translate_failure_parts,
        translate_observer_error,
    },
};

#[test]
fn admission_categories_are_exhaustive_and_definitely_unsent() {
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
fn failures_preserve_category_and_driver_authoritative_delivery() {
    for (kind, expected) in [
        (FailureKind::DeadlineElapsed, ErrorKind::Timeout),
        (FailureKind::DriverRejected, ErrorKind::Backpressure),
        (FailureKind::Transport, ErrorKind::Transport),
        (FailureKind::ResponseTooLarge, ErrorKind::Backpressure),
        (FailureKind::Compatibility, ErrorKind::Compatibility),
        (FailureKind::InvalidResponse, ErrorKind::Broker),
    ] {
        for (delivery, expected_delivery) in [
            (EngineDeliveryStatus::NotSent, DeliveryStatus::NotSent),
            (
                EngineDeliveryStatus::PossiblySent,
                DeliveryStatus::PossiblySent,
            ),
        ] {
            let error = translate_failure_parts(kind, delivery);
            assert_eq!(error.kind(), expected);
            assert_eq!(error.delivery_status(), Some(expected_delivery));
        }
    }
}

#[test]
fn signed_topic_and_partition_codes_are_lossless_and_zero_is_success() {
    assert!(broker_error(0, "topic").is_none());
    for (code, scope) in [(-731, "topic"), (731, "partition")] {
        let error = broker_error(code, scope).expect("nonzero broker error");
        assert_eq!(error.kind(), ErrorKind::Broker);
        assert_eq!(error.broker_code(), Some(code));
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
        assert!(error.to_string().contains(scope));
    }
}

#[test]
fn page_value_mapping_preserves_exact_api_75_fields_and_nullable_lists() {
    let partition = partition_from_parts(
        -32_000,
        7,
        None,
        Some(11),
        vec![9, 2],
        vec![2],
        Some(Vec::new()),
        None,
        vec![9],
    );
    let topic = topic_from_parts(
        -17,
        "orders".to_owned(),
        [0xAB; 16],
        true,
        vec![partition],
        i32::MIN,
    );
    let page = page_from_parts(31, vec![topic], Some(("orders".to_owned(), 8)));

    assert_eq!(page.throttle_time(), std::time::Duration::from_millis(31));
    let topic = &page.topics()[0];
    let topic_error = topic.error().expect("topic error");
    assert_eq!(topic_error.broker_code(), Some(-17));
    assert_eq!(topic_error.is_internal_topic(), Some(true));
    assert_eq!(topic.topic_id(), [0xAB; 16]);
    assert_eq!(topic.authorized_operations(), i32::MIN);

    let partition = &topic.partitions()[0];
    assert_eq!(
        partition.error().and_then(crate::KafkaError::broker_code),
        Some(-32_000)
    );
    assert_eq!(partition.partition_index(), 7);
    assert_eq!(partition.leader_id(), None);
    assert_eq!(partition.leader_epoch(), Some(11));
    assert_eq!(partition.replicas(), [9, 2]);
    assert_eq!(partition.in_sync_replicas(), [2]);
    assert_eq!(partition.eligible_leader_replicas(), Some(&[][..]));
    assert_eq!(partition.last_known_eligible_leader_replicas(), None);
    assert_eq!(partition.offline_replicas(), [9]);
    assert_eq!(
        page.next_cursor().map(|cursor| cursor.partition_index()),
        Some(8)
    );
}

#[test]
fn accepted_and_observer_failures_keep_stable_categories() {
    for fault in [AcceptedFaultKind::Wake, AcceptedFaultKind::HostInvariant] {
        assert_eq!(translate_accepted_fault(fault).kind(), ErrorKind::Internal);
    }
    assert_eq!(
        translate_observer_error(ObserverError::AlreadyObserved).kind(),
        ErrorKind::State
    );
    assert_eq!(
        translate_observer_error(ObserverError::Stale).kind(),
        ErrorKind::Internal
    );
}
