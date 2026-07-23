//! Submission route-provenance and pre-driver ownership rejection scenarios.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId, CompressionPolicy,
    Deadline, Moment, OperationId, PartitionIndex, ProducerEffect, TopicId,
};

use super::{
    ProducerRecord, ProducerStore, ProducerStoreLimits,
    binding::OperationBindings,
    execution::{PreparedExecution, PreparedExecutionError, PreparedExecutionLimits},
};

const fn batch_execution(batch_id: BatchId) -> BatchExecutionId {
    BatchExecutionId::new(batch_id, BatchExecutionGeneration::initial())
}

#[test]
fn submission_requires_prepared_bytes_before_deadline_ownership() {
    let mut execution = execution();
    let store = ProducerStore::new(store_limits());
    let bindings = OperationBindings::new(0);
    let batch_id = BatchId::from_raw(9);
    assert_eq!(
        execution.arm_submission(
            &store,
            &bindings,
            submission(
                batch_id,
                OperationId::from_raw(4),
                TopicId::from_raw(3),
                PartitionIndex::from_raw(7),
            ),
        ),
        Err(PreparedExecutionError::MissingPreparedBatch(
            batch_execution(batch_id)
        ))
    );
    assert_eq!(execution.submission_count(), 0);
}

#[test]
fn submission_route_mismatches_do_not_arm_or_discard_prepared_bytes() {
    let batch_id = BatchId::from_raw(1);
    let operation_id = OperationId::from_raw(4);
    let mut store = ProducerStore::new(store_limits());
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(7),
            100,
            None,
            Some(Bytes::from_static(b"value")),
        ))
        .unwrap_or_else(|error| panic!("record reservation failed: {error}"));
    let facts = store
        .commit(reservation)
        .unwrap_or_else(|error| panic!("record commit failed: {error}"));
    store
        .accumulate(batch_id, operation_id, facts.payload_id())
        .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
    let mut execution = execution();
    let bindings = OperationBindings::new(0);
    execution
        .materialize(
            &mut store,
            batch_execution(batch_id),
            CompressionPolicy::Uncompressed,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));

    let wrong_topic = TopicId::from_raw(facts.topic_id().get() + 1);
    let wrong_partition = PartitionIndex::from_raw(facts.partition().get() + 1);
    for (topic_id, partition) in [
        (wrong_topic, facts.partition()),
        (facts.topic_id(), wrong_partition),
    ] {
        assert!(matches!(
            execution.arm_submission(
                &store,
                &bindings,
                submission(batch_id, operation_id, topic_id, partition),
            ),
            Err(PreparedExecutionError::RouteMismatch {
                stored_topic_id,
                stored_partition,
                effect_topic_id,
                effect_partition,
                ..
            }) if stored_topic_id == facts.topic_id()
                && stored_partition == facts.partition()
                && effect_topic_id == topic_id
                && effect_partition == partition
        ));
    }
    assert_eq!(execution.submission_count(), 0);
    assert_eq!(execution.prepared_stats().batches, 1);
}

const fn submission(
    batch_id: BatchId,
    operation_id: OperationId,
    topic_id: TopicId,
    partition: PartitionIndex,
) -> ProducerEffect {
    ProducerEffect::SubmitProduce {
        execution: batch_execution(batch_id),
        deadline_operation_id: operation_id,
        deadline: Deadline::from_tick(20),
        topic_id,
        partition,
        acknowledgements: AcknowledgementPolicy::All,
    }
}

const fn execution() -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
        },
    )
}

const fn store_limits() -> ProducerStoreLimits {
    ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    }
}
