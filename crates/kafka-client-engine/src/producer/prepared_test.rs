//! Prepared-request capacity, ownership transfer, and exact release scenarios.

use bytes::Bytes;
use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId};

use super::{
    materialization::{MaterializationBatch, MaterializationRecord},
    prepared::{PreparedProduceError, PreparedProduceStats, PreparedProduceStore},
};
use crate::protocol::produce::materialize_explicit_produce_batch;

fn execution(value: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(value),
        BatchExecutionGeneration::initial(),
    )
}

#[test]
fn count_rejection_returns_the_unstored_request() {
    let mut store = PreparedProduceStore::new(1, usize::MAX);
    let first = prepared(b"first");
    let first_bytes = first.retained_record_bytes();
    store
        .insert(execution(1), first)
        .unwrap_or_else(|error| panic!("first insertion failed: {error}"));
    let second = prepared(b"second");
    let second_bytes = second.retained_record_bytes();
    let Err(rejected) = store.insert(execution(2), second) else {
        panic!("full count capacity should reject");
    };

    assert_eq!(rejected.reason(), PreparedProduceError::BatchCapacity);
    assert_eq!(rejected.into_value().retained_record_bytes(), second_bytes);
    assert_eq!(
        store.stats(),
        PreparedProduceStats {
            batches: 1,
            encoded_record_bytes: first_bytes,
        }
    );
}

#[test]
fn byte_rejection_returns_the_unstored_request() {
    let first = prepared(b"first");
    let first_bytes = first.retained_record_bytes();
    let candidate = prepared(b"payload");
    let candidate_bytes = candidate.retained_record_bytes();
    let capacity = first_bytes
        .checked_add(candidate_bytes)
        .and_then(|sum| sum.checked_sub(1))
        .unwrap_or_else(|| panic!("test byte capacity should be representable"));
    let mut store = PreparedProduceStore::new(2, capacity);
    store
        .insert(execution(1), first)
        .unwrap_or_else(|error| panic!("first insertion failed: {error}"));
    let Err(rejected) = store.insert(execution(2), candidate) else {
        panic!("undersized byte capacity should reject");
    };

    assert_eq!(rejected.reason(), PreparedProduceError::EncodedByteCapacity);
    assert_eq!(
        rejected.into_value().retained_record_bytes(),
        candidate_bytes
    );
    assert_eq!(
        store.stats(),
        PreparedProduceStats {
            batches: 1,
            encoded_record_bytes: first_bytes,
        }
    );
}

#[test]
fn duplicate_rejection_preserves_existing_and_incoming_requests() {
    let mut store = PreparedProduceStore::new(2, usize::MAX);
    let first = prepared(b"first");
    let first_bytes = first.retained_record_bytes();
    store
        .insert(execution(1), first)
        .unwrap_or_else(|error| panic!("first insertion failed: {error}"));
    let duplicate = prepared(b"duplicate");
    let duplicate_bytes = duplicate.retained_record_bytes();
    let Err(rejected) = store.insert(execution(1), duplicate) else {
        panic!("duplicate batch should reject");
    };

    assert_eq!(rejected.reason(), PreparedProduceError::DuplicateBatch);
    assert_eq!(
        rejected.into_value().retained_record_bytes(),
        duplicate_bytes
    );
    assert_eq!(store.stats().encoded_record_bytes, first_bytes);
}

#[test]
fn take_transfers_ownership_and_accounting_exactly_once() {
    let mut store = PreparedProduceStore::new(1, usize::MAX);
    let value = prepared(b"payload");
    let bytes = value.retained_record_bytes();
    store
        .insert(execution(9), value)
        .unwrap_or_else(|error| panic!("insertion failed: {error}"));

    let taken = store
        .take(execution(9))
        .unwrap_or_else(|error| panic!("take failed: {error}"));
    assert_eq!(taken.retained_record_bytes(), bytes);
    assert_eq!(store.stats().batches, 0);
    assert_eq!(store.stats().encoded_record_bytes, 0);
    assert!(matches!(
        store.take(execution(9)),
        Err(PreparedProduceError::UnknownBatch)
    ));
    assert_eq!(
        store.release(execution(9)),
        Err(PreparedProduceError::UnknownBatch)
    );
    assert!(
        store
            .insert(execution(10), prepared(b"replacement"))
            .is_ok()
    );
}

#[test]
fn release_drops_and_decrements_exactly_once() {
    let mut store = PreparedProduceStore::new(1, usize::MAX);
    let value = prepared(b"payload");
    let bytes = value.retained_record_bytes();
    store
        .insert(execution(3), value)
        .unwrap_or_else(|error| panic!("insertion failed: {error}"));

    assert_eq!(store.release(execution(3)), Ok(bytes));
    assert_eq!(
        store.release(execution(3)),
        Err(PreparedProduceError::UnknownBatch)
    );
    assert_eq!(store.stats().encoded_record_bytes, 0);
}

#[test]
fn stale_prepared_generation_cannot_take_or_release_current_bytes() {
    let mut store = PreparedProduceStore::new(1, usize::MAX);
    let current = execution(7);
    let stale = BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second generation must be valid")),
    );
    store
        .insert(current, prepared(b"current"))
        .unwrap_or_else(|error| panic!("insertion failed: {error}"));
    let before = store.stats();

    assert!(matches!(
        store.take(stale),
        Err(PreparedProduceError::ExecutionMismatch)
    ));
    assert_eq!(
        store.release(stale),
        Err(PreparedProduceError::ExecutionMismatch)
    );
    assert_eq!(store.stats(), before);
    assert!(store.contains(current));
}

fn prepared(value: &'static [u8]) -> crate::protocol::produce::MaterializedProduce {
    let input = MaterializationBatch::new(
        "orders".to_owned(),
        7,
        vec![MaterializationRecord::new(
            100,
            None,
            Some(Bytes::from_static(value)),
            Vec::new(),
        )],
        usize::MAX,
    );
    materialize_explicit_produce_batch(input)
        .unwrap_or_else(|error| panic!("test materialization failed: {error}"))
}
