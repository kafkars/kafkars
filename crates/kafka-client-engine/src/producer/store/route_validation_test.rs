//! Canonical record persistence for retry topic-identity validation.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, OperationId, PartitionIndex,
    partitioning::TopicMetadataGeneration,
};

use super::{ProducerStore, ProducerStoreLimits};
use crate::producer::record::{ProducerRecord, ProducerRecordParts, ProducerSourceOwner};

#[test]
fn newer_retry_validation_persists_and_rejects_the_same_generation_again() {
    let expected = [7; 16];
    let mut record = ProducerRecord::from_public(ProducerRecordParts {
        topic: Arc::from("orders"),
        expected_topic_uuid: Some(expected),
        partition: Some(PartitionIndex::from_raw(2)),
        timestamp_ms: 10,
        defaulted_timestamp: false,
        key: None,
        value: Some(Bytes::from_static(b"value")),
        headers: Vec::new(),
        source_owner: ProducerSourceOwner::none(),
    });
    assert!(record.validate_topic_uuid_at(Some(expected), TopicMetadataGeneration::from_raw(5),));
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(record)
        .unwrap_or_else(|error| panic!("reserve expected record: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("commit expected record: {error}"));
    let batch_id = BatchId::from_raw(1);
    store
        .accumulate(batch_id, OperationId::from_raw(1), facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulate expected record: {error}"));
    materialize(&mut store, execution(1));
    store
        .start_batch_retry(execution(1), execution(2))
        .unwrap_or_else(|error| panic!("start retry: {error}"));
    store
        .activate_batch_retry(execution(2))
        .unwrap_or_else(|error| panic!("activate retry: {error}"));
    materialize(&mut store, execution(2));
    let generation = TopicMetadataGeneration::from_raw(6);

    assert_eq!(
        store.record_retry_topic_identity(std::iter::once(execution(2)), expected, generation),
        Ok(true),
    );
    assert_eq!(
        store.can_record_retry_topic_identity(std::iter::once(execution(2)), expected, generation,),
        Ok(false),
    );
    assert_eq!(
        store
            .records
            .record(facts.payload_id())
            .map(ProducerRecord::validated_topic_generation),
        Ok(Some(generation)),
    );
}

fn materialize(store: &mut ProducerStore, execution: BatchExecutionId) {
    let (attempt, _batch) = store
        .materialization_view(execution, 1_024)
        .unwrap_or_else(|error| panic!("materialization view: {error}"));
    store
        .commit_materialization(attempt)
        .unwrap_or_else(|error| panic!("materialization commit: {error}"));
}

fn execution(generation: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(1),
        BatchExecutionGeneration::try_from_raw(generation)
            .unwrap_or_else(|| panic!("nonzero execution generation")),
    )
}
