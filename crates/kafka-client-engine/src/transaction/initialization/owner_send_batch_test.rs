//! Homogeneous transactional batch validation, recovery, and acceptance scenarios.

use std::time::Duration;

use bytes::Bytes;

use super::{TransactionSendAdmissionErrorKind, host_test::Fixture};
use crate::producer::PublicProducerRecord as ProducerRecord;

#[test]
fn deadline_capture_precedes_batch_validation_and_recovers_every_record() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let records = vec![
        ProducerRecord::to("").value(Bytes::from_static(b"first")),
        ProducerRecord::to("other")
            .partition(-1)
            .value(Bytes::from_static(b"second")),
    ];
    let Err(error) = transaction.send_batch(records, Duration::ZERO) else {
        panic!("zero-timeout batch was unexpectedly admitted")
    };

    assert_eq!(
        error.kind(),
        TransactionSendAdmissionErrorKind::InvalidDeadline
    );
    let records = error.into_records();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].value_bytes(),
        Some(&Bytes::from_static(b"first"))
    );
    assert_eq!(
        records[1].value_bytes(),
        Some(&Bytes::from_static(b"second"))
    );
}

#[test]
fn batch_requires_nonempty_one_topic_and_one_explicit_partition() {
    assert_rejected(Vec::new(), TransactionSendAdmissionErrorKind::EmptyBatch, 0);
    assert_rejected(
        vec![ProducerRecord::to("orders").value(Bytes::from_static(b"value"))],
        TransactionSendAdmissionErrorKind::MissingExplicitPartition,
        1,
    );
    assert_rejected(
        vec![
            record("orders", 2, b"first"),
            record("payments", 2, b"second"),
        ],
        TransactionSendAdmissionErrorKind::MixedBatchTopic,
        2,
    );
    assert_rejected(
        vec![
            record("orders", 2, b"first"),
            record("orders", 3, b"second"),
        ],
        TransactionSendAdmissionErrorKind::MixedBatchPartition,
        2,
    );
}

#[test]
fn accepted_batch_returns_one_borrowing_observer_for_the_whole_vector() {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    assert_eq!(transaction.batch_record_capacity(), 8);
    let accepted = transaction
        .send_batch(
            vec![
                record("orders", 2, b"first"),
                record("orders", 2, b"second"),
            ],
            Duration::from_secs(5),
        )
        .unwrap_or_else(|error| panic!("homogeneous batch admission: {error:?}"));

    assert!(!accepted.wake_failed());
    drop(accepted.into_observer());
}

fn assert_rejected(
    records: Vec<ProducerRecord>,
    expected: TransactionSendAdmissionErrorKind,
    expected_len: usize,
) {
    let fixture = Fixture::new();
    let mut owner = fixture.initialize(41);
    let mut transaction = owner
        .begin_transaction()
        .unwrap_or_else(|error| panic!("begin transaction: {error:?}"))
        .into_transaction();
    let Err(error) = transaction.send_batch(records, Duration::from_secs(5)) else {
        panic!("invalid homogeneous batch was unexpectedly admitted")
    };
    assert_eq!(error.kind(), expected);
    assert_eq!(error.into_records().len(), expected_len);
}

fn record(topic: &str, partition: i32, value: &'static [u8]) -> ProducerRecord {
    ProducerRecord::to(topic)
        .partition(partition)
        .value(Bytes::from_static(value))
}
