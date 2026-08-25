//! Public transactional record-send admission and ownership scenarios.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;

use super::{
    TransactionControlErrorKind, TransactionSendAdmissionErrorKind, host_test::Fixture,
    send_admission::record_error_kind,
};
use crate::producer::{PublicProducerRecord as ProducerRecord, TransactionRecordViewError};

#[test]
fn header_allocation_failure_maps_to_allocation_admission() {
    assert_eq!(
        record_error_kind(TransactionRecordViewError::Allocation),
        TransactionSendAdmissionErrorKind::Allocation
    );
}

#[test]
fn zero_timeout_returns_the_exact_original_record_before_validation() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let value = Bytes::from_static(b"same-allocation");
    let Err(error) = transaction.send(
        ProducerRecord::to("").partition(-1).value(value.clone()),
        Duration::ZERO,
    ) else {
        panic!("zero-timeout transactional send was unexpectedly admitted");
    };

    assert_eq!(
        error.kind(),
        TransactionSendAdmissionErrorKind::InvalidDeadline
    );
    let record = error.into_record();
    assert_eq!(record.topic(), "");
    assert_eq!(record.explicit_partition(), Some(-1));
    assert_eq!(record.value_bytes(), Some(&value));
}

#[test]
fn local_validation_returns_the_exact_original_record() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let value = Bytes::from_static(b"same-allocation");
    let Err(error) = transaction.send(
        ProducerRecord::to("orders")
            .partition(-1)
            .value(value.clone()),
        Duration::from_secs(5),
    ) else {
        panic!("negative transactional partition was unexpectedly accepted");
    };

    assert_eq!(
        error.kind(),
        TransactionSendAdmissionErrorKind::NegativeExplicitPartition
    );
    assert_eq!(error.record().value_bytes(), Some(&value));
    assert_eq!(error.into_record().topic(), "orders");
}

#[test]
fn explicit_capture_precedes_record_translation_and_is_consumed_by_admission() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let capture = transaction
        .capture_send(Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("capture transactional send: {error:?}"));
    let value = Bytes::from_static(b"same-allocation");
    let Err(error) = transaction.send_captured(
        ProducerRecord::to("orders")
            .partition(-1)
            .value(value.clone()),
        capture,
    ) else {
        panic!("captured negative transactional partition was unexpectedly accepted");
    };

    assert_eq!(
        error.kind(),
        TransactionSendAdmissionErrorKind::NegativeExplicitPartition
    );
    assert_eq!(error.into_record().value_bytes(), Some(&value));
}

#[test]
fn automatic_partition_send_crosses_acceptance_without_local_rejection() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let accepted = transaction
        .send(
            ProducerRecord::to("orders").value(Bytes::from_static(b"value")),
            Duration::from_secs(5),
        )
        .unwrap_or_else(|error| panic!("automatic route is resolved after acceptance: {error:?}"));

    assert!(!accepted.wake_failed());
    drop(accepted.into_observer());
}

#[test]
fn accepted_send_returns_one_token_borrowing_observer() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let accepted = transaction
        .send(
            ProducerRecord::to("orders")
                .partition(2)
                .value(Bytes::from_static(b"value")),
            Duration::from_secs(5),
        )
        .unwrap_or_else(|error| panic!("send admission: {error:?}"));

    assert!(!accepted.wake_failed());
    drop(accepted.into_observer());
}

#[test]
fn dropped_accepted_send_observer_remains_a_commit_preflight_fence() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let accepted = transaction
        .send(
            ProducerRecord::to("orders")
                .partition(2)
                .value(Bytes::from_static(b"value")),
            Duration::from_secs(5),
        )
        .unwrap_or_else(|error| panic!("send admission: {error:?}"));
    drop(accepted.into_observer());

    let error = transaction
        .preflight_commit()
        .err()
        .unwrap_or_else(|| panic!("dropped observation must not cancel accepted work"));
    assert_eq!(
        error.kind(),
        TransactionControlErrorKind::OutstandingOperations
    );
}

#[test]
fn accepted_single_releases_its_source_only_after_retained_byte_admission() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let source = Arc::new(());
    let source_weak = Arc::downgrade(&source);
    let record = ProducerRecord::to("orders")
        .partition(2)
        .value(Bytes::from_static(b"value"))
        .retain_source_owner(source);

    let accepted = transaction
        .send(record, Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("send admission: {error:?}"));

    assert!(source_weak.upgrade().is_none());
    drop(accepted.into_observer());
}
