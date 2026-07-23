//! Store-level evidence for ordered membership and route provenance.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex,
};

use super::{ProducerStore, ProducerStoreLimits};
use crate::producer::{
    ProducerRecord,
    batch_store::{BatchCancellationPhase, BatchRevisionExpectation},
};

#[test]
fn accumulated_record_preserves_exact_execution_route_and_membership() {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(3),
            7,
            None,
            Some(Bytes::from_static(b"value")),
        ))
        .unwrap_or_else(|error| panic!("record reservation failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("record commit failed: {error}"));
    let batch_id = BatchId::from_raw(5);
    let operation_id = OperationId::from_raw(9);
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("batch accumulation failed: {error}"));
    let execution = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());
    let (attempt, _view) = store
        .materialization_view(execution, 1_024)
        .unwrap_or_else(|error| panic!("materialization view failed: {error}"));
    store
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("materialization commit failed: {error}"));

    assert_eq!(
        store.execution_route(execution),
        Ok((facts.topic_id(), PartitionIndex::from_raw(3)))
    );
    assert_eq!(
        store.execution_contains_operation(execution, operation_id),
        Ok(true)
    );
}

#[test]
fn open_revision_wrappers_remove_exact_membership_atomically() {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(3),
            7,
            None,
            Some(Bytes::from_static(b"value")),
        ))
        .unwrap_or_else(|error| panic!("record reservation failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("record commit failed: {error}"));
    let batch_id = BatchId::from_raw(5);
    let operation_id = OperationId::from_raw(9);
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("batch accumulation failed: {error}"));
    let execution = BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial());

    assert_eq!(
        store.cancellation_phase(operation_id),
        Ok(Some(BatchCancellationPhase::Open(batch_id)))
    );
    let plan = store
        .plan_batch_revision(
            execution,
            operation_id,
            BatchRevisionExpectation::OpenForMaterialization,
        )
        .unwrap_or_else(|error| panic!("open revision preflight failed: {error}"));
    store.commit_batch_revision(plan);

    assert_eq!(store.cancellation_phase(operation_id), Ok(None));
    assert_eq!(store.stats().batches, 0);
}
