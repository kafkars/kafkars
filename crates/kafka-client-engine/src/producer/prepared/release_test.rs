//! Exact-generation prepared-byte release scenarios.

use bytes::Bytes;
use kafka_client_core::{BatchExecutionGeneration, BatchExecutionId, BatchId};

use super::{PreparedProduceError, PreparedProduceStore};
use crate::producer::materialization::{MaterializationBatch, MaterializationRecord};
use crate::protocol::produce::materialize_explicit_produce_batch;

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        batch_id,
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("test generation must be nonzero")),
    )
}

#[test]
fn stale_release_cannot_remove_replacement_prepared_bytes() {
    let batch_id = BatchId::from_raw(1);
    let stale = execution(batch_id, 1);
    let replacement = execution(batch_id, 2);
    let value = materialize_explicit_produce_batch(MaterializationBatch::new(
        "orders".to_owned(),
        0,
        vec![MaterializationRecord::new(
            0,
            None,
            Some(Bytes::from_static(b"value")),
            Vec::new(),
        )],
        usize::MAX,
    ))
    .unwrap_or_else(|error| panic!("test materialization failed: {error}"));
    let retained_bytes = value.retained_record_bytes();
    let mut store = PreparedProduceStore::new(1, usize::MAX);
    store
        .insert(replacement, value)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));

    assert_eq!(
        store.preflight_release(stale),
        Err(PreparedProduceError::ExecutionMismatch)
    );
    assert_eq!(
        store.release(stale),
        Err(PreparedProduceError::ExecutionMismatch)
    );
    assert!(store.contains(replacement));
    assert_eq!(store.stats().batches, 1);
    assert_eq!(store.stats().encoded_record_bytes, retained_bytes);
}
