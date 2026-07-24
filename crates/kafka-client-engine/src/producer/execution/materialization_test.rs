//! Materialization failure rollback and exact stale-commit scenarios.

use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, CompressionPolicy, Deadline, Moment,
    OperationId, PartitionIndex,
};

use super::{
    PreparedExecution, PreparedExecutionError, PreparedExecutionLimits, PreparedProduceError,
};
use crate::{
    clock::OperationDeadline,
    producer::{ProducerRecord, ProducerStore, ProducerStoreLimits},
    protocol::produce::materialize_explicit_produce_batch,
};

fn execution(batch_id: BatchId, generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        batch_id,
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("test generation must be nonzero")),
    )
}

fn store(batch_id: BatchId, operation_id: OperationId) -> ProducerStore {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(3),
            10,
            None,
            Some(Bytes::from_static(b"value")),
        ))
        .unwrap_or_else(|error| panic!("reservation failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit failed: {error}"));
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    store
}

fn prepared(max_batch_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes,
        },
    )
}

#[test]
fn stale_commit_drops_the_exact_prepared_bytes_it_inserted() {
    let batch_id = BatchId::from_raw(1);
    let operation_id = OperationId::from_raw(2);
    let current = execution(batch_id, 1);
    let replacement = execution(batch_id, 2);
    let mut store = store(batch_id, operation_id);
    let (attempt, input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("view failed: {error}"));
    let encoded = materialize_explicit_produce_batch(input)
        .unwrap_or_else(|error| panic!("encoding failed: {error}"));
    let mut prepared = prepared(1_024);
    prepared
        .retain_for_test(current, encoded)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    store.replace_batch_execution_for_test(batch_id, replacement);

    assert_eq!(
        prepared.commit_inserted(&mut store, attempt, Moment::from_tick(3)),
        Err(PreparedExecutionError::Store(
            crate::producer::ProducerStoreError::StaleBatchExecution
        ))
    );
    assert_eq!(prepared.prepared_stats().batches, 0);
    assert_eq!(prepared.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(store.batch_execution(batch_id), Ok(Some(replacement)));
}

#[test]
fn semantic_encoding_failure_returns_exact_attempt_to_ready() {
    let batch_id = BatchId::from_raw(6);
    let operation_id = OperationId::from_raw(7);
    let current = execution(batch_id, 1);
    let mut store = store(batch_id, operation_id);
    let mut prepared = prepared(1);

    assert_eq!(
        prepared.materialize(
            &mut store,
            current,
            CompressionPolicy::None,
            Moment::from_tick(8),
        ),
        Ok(kafka_client_core::ProducerInput::BatchMaterializationFailed { execution: current })
    );
    let (retry, _input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("ready retry failed: {error}"));
    assert!(matches!(
        store.abort_materialization(retry),
        crate::producer::batch_store::MaterializationAbort::Restored
    ));
}

#[test]
fn prepared_capacity_failure_returns_exact_attempt_to_ready() {
    let batch_id = BatchId::from_raw(9);
    let operation_id = OperationId::from_raw(10);
    let current = execution(batch_id, 1);
    let mut store = store(batch_id, operation_id);
    let mut prepared = PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1,
            max_batch_bytes: 1_024,
        },
    );

    assert_eq!(
        prepared.materialize(
            &mut store,
            current,
            CompressionPolicy::None,
            Moment::from_tick(11),
        ),
        Ok(kafka_client_core::ProducerInput::BatchMaterializationFailed { execution: current })
    );
    assert_eq!(prepared.prepared_stats().batches, 0);
    let (retry, _input) = store
        .materialization_view(current, 1_024)
        .unwrap_or_else(|error| panic!("ready retry failed: {error}"));
    assert!(matches!(
        store.abort_materialization(retry),
        crate::producer::batch_store::MaterializationAbort::Restored
    ));
}

#[test]
fn rollback_refuses_an_entry_already_armed_by_core() {
    let execution = execution(BatchId::from_raw(13), 1);
    let mut owner = prepared(1_024);
    let batch = crate::producer::materialization::MaterializationBatch::try_for_test(
        "orders".to_owned(),
        3,
        vec![
            crate::producer::materialization::MaterializationRecord::new(
                0,
                None,
                Some(Bytes::from_static(b"value")),
                Vec::new(),
            ),
        ],
        usize::MAX,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    let value = materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("encoding failed: {error}"));
    owner
        .retain_for_test(execution, value)
        .unwrap_or_else(|error| panic!("retention failed: {error}"));
    owner
        .arm_for_test(
            execution,
            OperationId::from_raw(14),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(20), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("arm failed: {error}"));
    let before = owner.prepared_stats();

    assert!(matches!(
        owner.take_unarmed_materialized(execution),
        Err(PreparedProduceError::SubmissionArmed)
    ));
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_count(), 1);
}

#[test]
fn prepared_entry_count_bound_rejects_without_accounting_drift() {
    let mut owner = prepared(1_024);
    let first = execution(BatchId::from_raw(15), 1);
    let second = execution(BatchId::from_raw(16), 1);
    let value = |partition| {
        let batch = crate::producer::materialization::MaterializationBatch::try_for_test(
            "orders".to_owned(),
            partition,
            vec![
                crate::producer::materialization::MaterializationRecord::new(
                    0,
                    None,
                    Some(Bytes::from_static(b"value")),
                    Vec::new(),
                ),
            ],
            usize::MAX,
        )
        .unwrap_or_else(|| panic!("test materialization batch must be representable"));
        materialize_explicit_produce_batch(batch)
            .unwrap_or_else(|error| panic!("encoding failed: {error}"))
    };
    owner
        .retain_for_test(first, value(1))
        .unwrap_or_else(|error| panic!("first retention failed: {error}"));
    let before = owner.prepared_stats();

    assert_eq!(
        owner.retain_for_test(second, value(2)),
        Err(PreparedProduceError::BatchCapacity)
    );
    assert_eq!(owner.prepared_stats(), before);
}

#[test]
fn duplicate_and_stale_insertions_return_incoming_bytes_without_touching_current_owner() {
    let batch_id = BatchId::from_raw(17);
    let current = execution(batch_id, 1);
    let stale = execution(batch_id, 2);
    let mut owner = prepared(1_024);
    let current_value = encoded_value(b"current");
    let current_pointer = current_value.encoded_records().as_ptr();
    owner
        .insert_materialized(current, current_value)
        .unwrap_or_else(|error| panic!("current retention failed: {error:?}"));
    let before = owner.prepared_stats();

    for (execution, reason, bytes) in [
        (
            current,
            PreparedProduceError::DuplicateBatch,
            b"duplicate".as_slice(),
        ),
        (
            stale,
            PreparedProduceError::ExecutionMismatch,
            b"stale".as_slice(),
        ),
    ] {
        let incoming = encoded_value(bytes);
        let incoming_pointer = incoming.encoded_records().as_ptr();
        let Err(rejected) = owner.insert_materialized(execution, incoming) else {
            panic!("conflicting insertion must return its incoming owner")
        };
        assert_eq!(rejected.reason(), reason);
        assert_eq!(
            rejected.into_materialized().encoded_records().as_ptr(),
            incoming_pointer
        );
        assert_eq!(owner.prepared_stats(), before);
        let retained = owner
            .entries
            .get(&batch_id)
            .unwrap_or_else(|| panic!("current entry must remain retained"));
        assert_eq!(retained.execution, current);
        assert_eq!(
            retained.materialized.encoded_records().as_ptr(),
            current_pointer
        );
    }
}

fn encoded_value(value: &'static [u8]) -> crate::protocol::produce::MaterializedProduce {
    let batch = crate::producer::materialization::MaterializationBatch::try_for_test(
        "orders".to_owned(),
        3,
        vec![
            crate::producer::materialization::MaterializationRecord::new(
                0,
                None,
                Some(Bytes::from_static(value)),
                Vec::new(),
            ),
        ],
        usize::MAX,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("encoding failed: {error}"))
}
