//! Ordered batch membership, capacity, removal, and release scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{BatchId, OperationId, PartitionIndex};

use super::{ProducerRecord, ProducerStore, ProducerStoreError, ProducerStoreLimits};

#[test]
fn batch_count_is_bounded_independently_of_record_capacity() {
    let mut store = ProducerStore::new(limits(4, 128, 1));
    let first = admit(&mut store, "orders", 0, b"a");
    let second = admit(&mut store, "orders", 1, b"b");
    assert_eq!(
        store.accumulate(
            BatchId::from_raw(1),
            OperationId::from_raw(1),
            first.payload_id(),
        ),
        Ok(first.retained_bytes())
    );
    assert_eq!(
        store.accumulate(
            BatchId::from_raw(2),
            OperationId::from_raw(2),
            second.payload_id(),
        ),
        Err(ProducerStoreError::BatchCapacity)
    );
    assert_eq!(store.stats().batches, 1);
}

#[test]
fn release_batch_precedes_payload_release_and_is_exactly_once() {
    let mut store = ProducerStore::new(limits(2, 128, 2));
    let facts = admit(&mut store, "orders", 0, b"a");
    let batch_id = BatchId::from_raw(7);
    store
        .accumulate(batch_id, OperationId::from_raw(9), facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));

    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Err(ProducerStoreError::PayloadStillBatched)
    );
    assert_eq!(store.release_batch(batch_id), Ok(()));
    assert_eq!(
        store.release_batch(batch_id),
        Err(ProducerStoreError::UnknownBatch)
    );
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
}

#[test]
fn removing_the_last_member_does_not_retain_an_empty_batch() {
    let mut store = ProducerStore::new(limits(1, 64, 1));
    let facts = admit(&mut store, "orders", 0, b"a");
    let batch_id = BatchId::from_raw(3);
    let operation_id = OperationId::from_raw(4);
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));

    assert_eq!(
        store.remove_member(batch_id, operation_id),
        Ok(facts.payload_id())
    );
    assert_eq!(store.stats().batches, 0);
    assert_eq!(
        store.release_payload(facts.payload_id(), facts.retained_bytes()),
        Ok(())
    );
}

#[test]
fn operation_and_payload_membership_cannot_be_duplicated() {
    let mut store = ProducerStore::new(limits(3, 128, 3));
    let first = admit(&mut store, "orders", 0, b"a");
    let second = admit(&mut store, "orders", 0, b"b");
    let batch = BatchId::from_raw(1);
    let operation = OperationId::from_raw(1);
    store
        .accumulate(batch, operation, first.payload_id())
        .unwrap_or_else(|error| panic!("first accumulation failed: {error}"));

    assert_eq!(
        store.accumulate(BatchId::from_raw(2), operation, second.payload_id()),
        Err(ProducerStoreError::DuplicateOperation)
    );
    assert_eq!(
        store.accumulate(
            BatchId::from_raw(2),
            OperationId::from_raw(2),
            first.payload_id(),
        ),
        Err(ProducerStoreError::DuplicatePayloadMembership)
    );
}

#[test]
fn catalog_identity_batches_equal_names_and_fences_different_names() {
    let mut store = ProducerStore::new(limits(3, 128, 2));
    let first = admit(&mut store, "orders", 0, b"a");
    let second = admit(&mut store, "orders", 0, b"b");
    let different = admit(&mut store, "payments", 0, b"c");
    let batch = BatchId::from_raw(1);
    store
        .accumulate(batch, OperationId::from_raw(1), first.payload_id())
        .unwrap_or_else(|error| panic!("first accumulation failed: {error}"));
    assert!(
        store
            .accumulate(batch, OperationId::from_raw(2), second.payload_id())
            .is_ok()
    );
    assert_eq!(
        store.accumulate(batch, OperationId::from_raw(3), different.payload_id(),),
        Err(ProducerStoreError::BatchRouteMismatch)
    );
}

fn admit(
    store: &mut ProducerStore,
    topic: &str,
    partition: u32,
    value: &'static [u8],
) -> kafka_client_core::ExplicitRecord {
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from(topic),
            PartitionIndex::from_raw(partition),
            10,
            None,
            Some(Bytes::from_static(value)),
        ))
        .unwrap_or_else(|error| panic!("reserve failed: {error}"));
    store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"))
}

const fn limits(max_records: usize, max_bytes: usize, max_batches: usize) -> ProducerStoreLimits {
    ProducerStoreLimits {
        records: max_records,
        bytes: max_bytes,
        batches: max_batches,
    }
}
