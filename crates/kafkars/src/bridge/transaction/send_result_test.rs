//! Exhaustive transactional-send admission and terminal translation.

use kafka_client_engine::{
    TransactionControlErrorKind, TransactionSendAdmissionErrorKind, TransactionSendConsequence,
    TransactionSendDeliveryStatus, TransactionSendFailureKind, TransactionSendObserverError,
};

use super::send_result::{
    translate_send_admission, translate_send_failure_kind, translate_send_failure_parts,
    translate_send_metadata_parts, translate_send_observation,
};
use crate::{DeliveryStatus, ErrorKind};

#[test]
fn every_send_admission_kind_has_one_stable_facade_category() {
    use TransactionSendAdmissionErrorKind as Kind;
    let cases = [
        (Kind::InvalidDeadline, ErrorKind::Timeout),
        (Kind::TimestampUnavailable, ErrorKind::Internal),
        (Kind::EmptyTopic, ErrorKind::InvalidRecord),
        (Kind::NegativeExplicitPartition, ErrorKind::InvalidRecord),
        (Kind::RetainedSizeOverflow, ErrorKind::InvalidRecord),
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::Closed, ErrorKind::State),
        (Kind::StaleOwner, ErrorKind::State),
        (
            Kind::RetainedRecordBytes {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
        ),
        (
            Kind::RetainedTopicCapacity {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
        ),
        (
            Kind::RetainedTopicBytes {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
        ),
        (Kind::RetainedTopicBytesOverflow, ErrorKind::Internal),
        (Kind::TopicIdentityExhausted, ErrorKind::Internal),
        (Kind::Allocation, ErrorKind::Backpressure),
        (Kind::Busy, ErrorKind::Backpressure),
        (Kind::SendIdentityExhausted, ErrorKind::Internal),
        (Kind::InvalidPartition, ErrorKind::InvalidRecord),
        (
            Kind::Transaction(TransactionControlErrorKind::Fenced),
            ErrorKind::Fenced,
        ),
    ];

    for (kind, expected) in cases {
        let error = translate_send_admission(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert!(!error.requires_transaction_abort());
    }
}

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
        TransactionSendFailureKind::Transport,
        TransactionSendDeliveryStatus::NotSent,
        None,
        TransactionSendConsequence::AbortRequired,
    );
    assert_eq!(abort_required.kind(), ErrorKind::Transport);
    assert!(abort_required.requires_transaction_abort());

    let fatal = translate_send_failure_parts(
        TransactionSendFailureKind::Broker,
        TransactionSendDeliveryStatus::PossiblySent,
        Some(90),
        TransactionSendConsequence::Fatal,
    );
    assert_eq!(fatal.kind(), ErrorKind::Fenced);
    assert_eq!(fatal.broker_code(), Some(90));
    assert!(!fatal.requires_transaction_abort());
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
