//! Record reservation, rollback, accounting, and identity scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use kafka_client_core::{ByteCount, PartitionIndex};

use super::{
    ProducerRecord, ProducerStore, ProducerStoreError, ProducerStoreLimits, ProducerStoreStats,
    record::{ProducerHeader, ProducerRecordParts, ProducerSourceOwner},
};

#[test]
fn count_and_bytes_are_reserved_before_core_admission() {
    let mut store = ProducerStore::new(limits(1, 8, 1));
    let first = store
        .reserve(record("orders", Some(Bytes::new()), Some(b"v"), Vec::new()))
        .unwrap_or_else(|error| panic!("first record should reserve: {error}"));
    assert_eq!(first.facts().retained_bytes(), ByteCount::new(7));
    assert_eq!(
        store.stats(),
        ProducerStoreStats {
            records: 1,
            bytes: 7,
            batches: 0,
            topics: 1,
        }
    );

    let Err(rejected) = store.reserve(record("other", None, None, Vec::new())) else {
        panic!("record count should reject before core admission");
    };
    assert_eq!(rejected.reason(), ProducerStoreError::RecordCapacity);
    assert_eq!(rejected.into_record().topic().as_ref(), "other");
    assert_eq!(store.stats().records, 1);

    let (rolled_back, cleanup) = store.rollback(first).into_parts();
    assert_eq!(cleanup, Ok(()));
    assert_eq!(rolled_back.topic().as_ref(), "orders");
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn byte_rejection_returns_the_same_linear_record() {
    let topic: Arc<str> = Arc::from("orders");
    let mut store = ProducerStore::new(limits(4, 6, 4));
    let Err(rejected) = store.reserve(
        ProducerRecord::new(
            Arc::clone(&topic),
            PartitionIndex::from_raw(0),
            10,
            Some(Bytes::new()),
            Some(Bytes::from_static(b"x")),
        )
        .with_headers(vec![
            ProducerHeader::new(String::new(), None),
            ProducerHeader::new(String::new(), Some(Bytes::new())),
        ]),
    ) else {
        panic!("seven retained bytes must exceed the six byte bound");
    };
    assert_eq!(rejected.reason(), ProducerStoreError::ByteCapacity);
    let returned = rejected.into_record();

    assert!(Arc::ptr_eq(returned.topic(), &topic));
    let (_topic, timestamp, key, value, headers) = returned.into_parts();
    assert_eq!(timestamp, 10);
    assert_eq!(key.as_deref(), Some(&b""[..]));
    assert_eq!(value.as_deref(), Some(&b"x"[..]));
    let mut headers = headers.into_iter();
    assert!(
        headers
            .next()
            .unwrap_or_else(|| panic!("first header missing"))
            .into_parts()
            .1
            .is_none()
    );
    assert_eq!(
        headers
            .next()
            .unwrap_or_else(|| panic!("second header missing"))
            .into_parts()
            .1
            .as_deref(),
        Some(&b""[..])
    );
    assert_eq!(store.stats().records, 0);
}

#[test]
fn rollback_restores_capacity_without_reusing_payload_identity() {
    let mut store = ProducerStore::new(limits(1, 64, 1));
    let first = store
        .reserve(record("orders", None, Some(b"a"), Vec::new()))
        .unwrap_or_else(|error| panic!("first reserve failed: {error}"));
    let first_id = first.facts().payload_id();
    let (returned, cleanup) = store.rollback(first).into_parts();
    assert_eq!(cleanup, Ok(()));
    let second = store
        .reserve(returned)
        .unwrap_or_else(|error| panic!("second reserve failed: {error}"));

    assert!(second.facts().payload_id().get() > first_id.get());
    assert_eq!(store.stats().records, 1);
}

#[test]
fn reservation_keeps_record_bytes_outside_the_fallible_slot_until_commit() {
    let value = Bytes::from_static(b"linearly-owned");
    let mut records = super::record_store::RecordStore::new(1, 64);
    let reservation = records
        .reserve(record("orders", None, Some(b"linearly-owned"), Vec::new()))
        .unwrap_or_else(|error| panic!("record should reserve: {error}"));
    let payload_id = reservation.facts().payload_id();

    let slot = records
        .slots
        .get(&payload_id)
        .unwrap_or_else(|| panic!("reserved accounting slot should exist"));
    assert!(slot.record.is_none());
    let (returned, cleanup) = records.rollback(reservation).into_parts();

    assert_eq!(cleanup, Ok(()));
    let (_topic, _timestamp, _key, returned_value, _headers) = returned.into_parts();
    assert_eq!(
        returned_value.as_ref().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
}

#[test]
fn commit_releases_source_owner_only_after_retained_bytes_are_charged() {
    let dropped = Arc::new(AtomicBool::new(false));
    let source_owner: Arc<dyn Send + Sync> = Arc::new(DropSentinel(Arc::clone(&dropped)));
    let mut records = super::record_store::RecordStore::new(1, 64);
    let reservation = records
        .reserve(ProducerRecord::from_public(ProducerRecordParts {
            topic: Arc::from("orders"),
            expected_topic_uuid: None,
            partition: Some(PartitionIndex::from_raw(0)),
            timestamp_ms: 10,
            defaulted_timestamp: false,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers: Vec::new(),
            source_owner: ProducerSourceOwner::new(source_owner),
        }))
        .unwrap_or_else(|error| panic!("record should reserve: {error}"));
    let retained_bytes = reservation.facts().retained_bytes().get();

    assert_eq!(u64::try_from(records.used_bytes()), Ok(retained_bytes));
    assert!(!dropped.load(Ordering::Acquire));
    records
        .commit(reservation)
        .unwrap_or_else(|error| panic!("charged reservation should commit: {error}"));
    assert_eq!(u64::try_from(records.used_bytes()), Ok(retained_bytes));
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn cleanup_corruption_cannot_consume_the_reserved_record() {
    let value = Bytes::from_static(b"must-return");
    let mut records = super::record_store::RecordStore::new(1, 64);
    let reservation = records
        .reserve(record("orders", None, Some(b"must-return"), Vec::new()))
        .unwrap_or_else(|error| panic!("record should reserve: {error}"));
    let payload_id = reservation.facts().payload_id();
    let _corrupted_slot = records.slots.remove(&payload_id);

    let (returned, cleanup) = records.rollback(reservation).into_parts();
    let (_topic, _timestamp, _key, returned_value, _headers) = returned.into_parts();

    assert_eq!(cleanup, Err(ProducerStoreError::UnknownPayload));
    assert_eq!(
        returned_value.as_ref().map(|bytes| bytes.as_ptr()),
        Some(value.as_ptr())
    );
}

#[test]
fn release_checks_provenance_and_happens_exactly_once() {
    let mut store = ProducerStore::new(limits(1, 64, 1));
    let reservation = store
        .reserve(record("orders", None, Some(b"a"), Vec::new()))
        .unwrap_or_else(|error| panic!("reserve failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));

    assert_eq!(
        store.release_payload(facts.payload_id(), ByteCount::new(99)),
        Err(ProducerStoreError::RetainedSizeMismatch)
    );
    assert_eq!(store.stats().records, 1);
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Err(ProducerStoreError::UnknownPayload)
    );
    assert_eq!(store.topic_count(), 0);
}

fn limits(max_records: usize, max_bytes: usize, max_batches: usize) -> ProducerStoreLimits {
    ProducerStoreLimits {
        records: max_records,
        bytes: max_bytes,
        batches: max_batches,
    }
}

fn record(
    topic: &str,
    key: Option<Bytes>,
    value: Option<&'static [u8]>,
    headers: Vec<ProducerHeader>,
) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        10,
        key,
        value.map(Bytes::from_static),
    )
    .with_headers(headers)
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
