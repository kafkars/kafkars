//! Exhaustive transactional-send admission and terminal translation.

use kafka_client_engine::{
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailureKind,
    TransactionSendObserverError,
};

use super::send_result::{
    translate_send_failure_kind, translate_send_failure_parts, translate_send_metadata_parts,
    translate_send_observation, translate_topic_uuid,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn every_terminal_failure_kind_has_one_stable_facade_category() {
    use TransactionSendFailureKind as Kind;
    let cases = [
        (Kind::Busy, ErrorKind::Backpressure),
        (Kind::StaleTransaction, ErrorKind::State),
        (Kind::OwnerUnavailable, ErrorKind::State),
        (Kind::InvalidTarget, ErrorKind::InvalidRecord),
        (Kind::Backpressure, ErrorKind::Backpressure),
        (Kind::DeadlineElapsed, ErrorKind::Timeout),
        (Kind::DriverRejected, ErrorKind::Backpressure),
        (Kind::Transport, ErrorKind::Transport),
        (Kind::Compatibility, ErrorKind::Compatibility),
        (Kind::InvalidResponse, ErrorKind::Broker),
        (Kind::DriverClosed, ErrorKind::Internal),
        (Kind::Broker, ErrorKind::Broker),
        (Kind::Access, ErrorKind::Access),
        (Kind::Coordinator, ErrorKind::Routing),
        (Kind::Fenced, ErrorKind::Broker),
        (Kind::InvalidRecord, ErrorKind::InvalidRecord),
        (Kind::Identity, ErrorKind::Identity),
        (Kind::ProducerIdentity, ErrorKind::State),
        (Kind::Materialization, ErrorKind::Internal),
        (Kind::Routing, ErrorKind::Routing),
        (Kind::NameResolution, ErrorKind::Transport),
        (Kind::ConnectionUnavailable, ErrorKind::Transport),
        (Kind::Permanent, ErrorKind::Internal),
        (Kind::Correlation, ErrorKind::Internal),
    ];

    for (kind, expected) in cases {
        assert_eq!(translate_send_failure_kind(kind), expected);
    }
}

#[test]
fn consequence_delivery_and_broker_code_remain_lossless() {
    let healthy = translate_send_failure_parts(
        TransactionSendFailureKind::Broker,
        TransactionSendDeliveryStatus::PossiblySent,
        Some(-123),
        TransactionSendConsequence::FailedHealthy,
    );
    assert_eq!(healthy.kind(), ErrorKind::Broker);
    assert_eq!(
        healthy.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
    assert_eq!(healthy.broker_code(), Some(-123));
    assert!(!healthy.requires_transaction_abort());

    let abort_required = translate_send_failure_parts(
        TransactionSendFailureKind::Identity,
        TransactionSendDeliveryStatus::NotSent,
        None,
        TransactionSendConsequence::AbortRequired,
    );
    assert_eq!(abort_required.kind(), ErrorKind::Identity);
    assert_eq!(
        abort_required.delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
    assert!(abort_required.requires_transaction_abort());
    assert!(!abort_required.is_fatal());

    let fenced = translate_send_failure_parts(
        TransactionSendFailureKind::Fenced,
        TransactionSendDeliveryStatus::PossiblySent,
        Some(90),
        TransactionSendConsequence::Fatal,
    );
    assert_eq!(fenced.kind(), ErrorKind::Fenced);
    assert_eq!(fenced.broker_code(), Some(90));
    assert!(fenced.is_fatal());
    assert!(!fenced.requires_transaction_abort());

    for (kind, code, expected) in [
        (
            TransactionSendFailureKind::Broker,
            Some(-123),
            ErrorKind::Broker,
        ),
        (
            TransactionSendFailureKind::Fenced,
            Some(91),
            ErrorKind::Broker,
        ),
        (TransactionSendFailureKind::Fenced, None, ErrorKind::Broker),
        (
            TransactionSendFailureKind::Transport,
            None,
            ErrorKind::Transport,
        ),
        (
            TransactionSendFailureKind::Access,
            Some(53),
            ErrorKind::Access,
        ),
        (
            TransactionSendFailureKind::Coordinator,
            Some(16),
            ErrorKind::Routing,
        ),
    ] {
        let error = translate_send_failure_parts(
            kind,
            TransactionSendDeliveryStatus::PossiblySent,
            code,
            TransactionSendConsequence::Fatal,
        );
        assert_eq!(error.kind(), expected, "{kind:?}");
        assert_eq!(error.broker_code(), code);
        assert!(error.is_fatal());
        assert_ne!(error.kind(), ErrorKind::Fenced);
    }
}

#[test]
fn observer_failures_translate_without_inventing_transaction_consequences() {
    for (observer, expected) in [
        (
            TransactionSendObserverError::AlreadyObserved,
            ErrorKind::State,
        ),
        (TransactionSendObserverError::Stale, ErrorKind::State),
        (
            TransactionSendObserverError::InternalInvariant,
            ErrorKind::Internal,
        ),
    ] {
        let Err(error) = translate_send_observation(Err(observer), Some(8), Some(7)) else {
            panic!("observer failure must remain an error")
        };
        assert_eq!(error.kind(), expected);
        assert!(!error.requires_transaction_abort());
        assert_eq!(error.delivery_status(), None);
    }
}

#[test]
fn transactional_metadata_preserves_exact_null_empty_and_nonempty_sizes() {
    for (key_size, value_size) in [(None, None), (Some(0), Some(0)), (Some(8), Some(7))] {
        let metadata = translate_send_metadata_parts(
            "orders".to_owned(),
            None,
            2,
            91,
            Some(1_700_000_000_456),
            Some(7),
            key_size,
            value_size,
        );

        assert_eq!(metadata.serialized_key_size(), key_size);
        assert_eq!(metadata.serialized_value_size(), value_size);
    }
}

#[test]
fn zero_uuid_on_engine_success_fails_closed_with_possibly_sent_certainty() {
    let error = translate_topic_uuid(Some([0; 16]))
        .err()
        .unwrap_or_else(|| panic!("zero engine UUID must not become absent facade evidence"));

    assert_eq!(error.kind(), ErrorKind::Identity);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert!(error.requires_transaction_abort());
    assert!(error.is_fatal());
}
