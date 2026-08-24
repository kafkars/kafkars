//! Source-lease preservation through homogeneous batch admission and rejection.

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use bytes::Bytes;

use super::{TransactionSendAdmissionErrorKind, host_test::Fixture};
use crate::producer::PublicProducerRecord as ProducerRecord;

#[test]
fn empty_mixed_and_capacity_rejections_return_every_exact_source_lease() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();

    let Err(empty) = transaction.send_batch(Vec::new(), Duration::from_secs(5)) else {
        panic!("empty batch was unexpectedly admitted")
    };
    assert_eq!(empty.kind(), TransactionSendAdmissionErrorKind::EmptyBatch);
    assert!(empty.into_records().is_empty());

    let (first, first_weak) = leased_record("orders", 2, b"first");
    let (second, second_weak) = leased_record("payments", 2, b"second");
    let Err(mixed) = transaction.send_batch(vec![first, second], Duration::from_secs(5)) else {
        panic!("mixed-topic batch was unexpectedly admitted")
    };
    assert_eq!(
        mixed.kind(),
        TransactionSendAdmissionErrorKind::MixedBatchTopic
    );
    assert_all_retained([&first_weak, &second_weak]);
    let records = mixed.into_records();
    assert_eq!(records[0].topic(), "orders");
    assert_eq!(records[1].topic(), "payments");
    assert_all_retained([&first_weak, &second_weak]);
    drop(records);
    assert_all_released([&first_weak, &second_weak]);

    let (records, weak) = leased_records(9);
    let Err(over_capacity) = transaction.send_batch(records, Duration::from_secs(5)) else {
        panic!("over-capacity batch was unexpectedly admitted")
    };
    assert_eq!(
        over_capacity.kind(),
        TransactionSendAdmissionErrorKind::BatchRecordCapacity {
            actual: 9,
            limit: 8,
        }
    );
    assert_all_retained(&weak);
    let records = over_capacity.into_records();
    assert_eq!(records.len(), 9);
    assert_all_retained(&weak);
    drop(records);
    assert_all_released(&weak);
}

#[test]
fn retained_byte_and_busy_rejections_preserve_sources_while_acceptance_releases_them() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();

    let (first, accepted_weak) = leased_record("orders", 2, b"accepted");
    let accepted = transaction
        .send_batch(vec![first], Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("batch admission: {error:?}"));
    assert!(accepted_weak.upgrade().is_none());
    drop(accepted.into_observer());

    let (busy_record, busy_weak) = leased_record("orders", 2, b"busy");
    let Err(busy) = transaction.send_batch(vec![busy_record], Duration::from_secs(5)) else {
        panic!("second batch unexpectedly acquired the fixed send slot")
    };
    assert_eq!(busy.kind(), TransactionSendAdmissionErrorKind::Busy);
    assert!(busy_weak.upgrade().is_some());
    let records = busy.into_records();
    assert_eq!(records[0].value_bytes(), Some(&Bytes::from_static(b"busy")));
    assert!(busy_weak.upgrade().is_some());
    drop(records);
    assert!(busy_weak.upgrade().is_none());
}

#[test]
fn retained_byte_rejection_keeps_the_exact_batch_source_owned() {
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
        .value(Bytes::from(vec![0; 70 * 1_024]))
        .retain_source_owner(source);

    let Err(error) = transaction.send_batch(vec![record], Duration::from_secs(5)) else {
        panic!("over-retained-byte batch was unexpectedly admitted")
    };
    assert!(matches!(
        error.kind(),
        TransactionSendAdmissionErrorKind::RetainedRecordBytes { .. }
    ));
    assert!(source_weak.upgrade().is_some());
    let records = error.into_records();
    assert_eq!(records.len(), 1);
    assert!(source_weak.upgrade().is_some());
    drop(records);
    assert!(source_weak.upgrade().is_none());
}

fn leased_record(topic: &str, partition: i32, value: &'static [u8]) -> (ProducerRecord, Weak<()>) {
    let source = Arc::new(());
    let weak = Arc::downgrade(&source);
    (
        ProducerRecord::to(topic)
            .partition(partition)
            .value(Bytes::from_static(value))
            .retain_source_owner(source),
        weak,
    )
}

fn leased_records(count: usize) -> (Vec<ProducerRecord>, Vec<Weak<()>>) {
    (0..count)
        .map(|_| leased_record("orders", 2, b"value"))
        .unzip()
}

fn assert_all_retained<'a>(weak: impl IntoIterator<Item = &'a Weak<()>>) {
    assert!(weak.into_iter().all(|source| source.upgrade().is_some()));
}

fn assert_all_released<'a>(weak: impl IntoIterator<Item = &'a Weak<()>>) {
    assert!(weak.into_iter().all(|source| source.upgrade().is_none()));
}
