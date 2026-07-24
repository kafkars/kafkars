//! Admission-bound submission deadline preservation and disagreement scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{
    AcknowledgementPolicy, BatchExecutionGeneration, BatchExecutionId, BatchId, CompressionPolicy,
    Deadline, Moment, OperationId, PartitionIndex, ProducerEffect,
};

use super::{PreparedExecution, PreparedExecutionError, PreparedExecutionLimits};
use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    producer::{ProducerRecord, ProducerStore, ProducerStoreLimits, binding::OperationBindings},
};

#[test]
fn submission_requires_and_preserves_the_admitted_operation_deadline() {
    let operation_id = OperationId::from_raw(7);
    let execution_id =
        BatchExecutionId::new(BatchId::from_raw(3), BatchExecutionGeneration::initial());
    let (mut store, topic_id) = store(execution_id.batch_id(), operation_id);
    let mut execution = PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
        },
    );
    execution
        .materialize(
            &mut store,
            execution_id,
            CompressionPolicy::None,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("materialization failed: {error}"));
    let effect = submission(
        execution_id,
        operation_id,
        topic_id,
        Deadline::from_tick(20),
    );

    assert_eq!(
        execution.arm_submission(&store, &OperationBindings::new(0), effect),
        Err(PreparedExecutionError::UnknownDeadlineOperation(
            operation_id
        ))
    );

    let mut mismatched = OperationBindings::new(1);
    mismatched
        .bind(
            operation_id,
            CompletionId::from_parts_for_test(0, 1),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(21), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("mismatched binding failed: {error}"));
    assert!(matches!(
        execution.arm_submission(&store, &mismatched, effect),
        Err(PreparedExecutionError::DeadlineMismatch {
            operation_id: actual,
            effect: actual_effect,
            bound: actual_bound,
        }) if actual == operation_id
            && actual_effect == Deadline::from_tick(20)
            && actual_bound == Deadline::from_tick(21)
    ));

    let Some(transport) = Instant::now().checked_add(Duration::from_secs(20)) else {
        panic!("small monotonic addition should be representable");
    };
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(20), transport);
    let mut bindings = OperationBindings::new(1);
    bindings
        .bind(
            operation_id,
            CompletionId::from_parts_for_test(0, 2),
            deadline,
        )
        .unwrap_or_else(|error| panic!("operation binding failed: {error}"));
    assert_eq!(execution.arm_submission(&store, &bindings, effect), Ok(()));
    assert_eq!(execution.submission_deadline(execution_id), Some(deadline));
}

#[test]
fn equal_deadline_from_another_batch_cannot_arm_this_execution() {
    let target_operation = OperationId::from_raw(31);
    let foreign_operation = OperationId::from_raw(32);
    let target_execution =
        BatchExecutionId::new(BatchId::from_raw(33), BatchExecutionGeneration::initial());
    let foreign_execution =
        BatchExecutionId::new(BatchId::from_raw(34), BatchExecutionGeneration::initial());
    let (mut store, topic_id) = two_batch_store(
        target_execution,
        target_operation,
        foreign_execution,
        foreign_operation,
    );
    let mut execution = PreparedExecution::new(
        2,
        PreparedExecutionLimits {
            encoded_bytes: 2_048,
            max_batch_bytes: 1_024,
        },
    );
    execution
        .materialize(
            &mut store,
            target_execution,
            CompressionPolicy::None,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("target materialization failed: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(40), Instant::now());
    let mut bindings = OperationBindings::new(1);
    bindings
        .bind(
            foreign_operation,
            CompletionId::from_parts_for_test(0, 1),
            deadline,
        )
        .unwrap_or_else(|error| panic!("foreign binding failed: {error}"));

    assert_eq!(
        execution.arm_submission(
            &store,
            &bindings,
            submission(
                target_execution,
                foreign_operation,
                topic_id,
                deadline.core(),
            ),
        ),
        Err(PreparedExecutionError::DeadlineOperationMismatch {
            execution: target_execution,
            operation_id: foreign_operation,
        })
    );
    assert_eq!(execution.submission_deadline(target_execution), None);
}

fn store(
    batch_id: BatchId,
    operation_id: OperationId,
) -> (ProducerStore, kafka_client_core::TopicId) {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 1,
        bytes: 1_024,
        batches: 1,
    });
    let reservation = store
        .reserve(ProducerRecord::new(
            Arc::from("orders"),
            PartitionIndex::from_raw(4),
            10,
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
    (store, facts.topic_id())
}

fn two_batch_store(
    target_execution: BatchExecutionId,
    target_operation: OperationId,
    foreign_execution: BatchExecutionId,
    foreign_operation: OperationId,
) -> (ProducerStore, kafka_client_core::TopicId) {
    let mut store = ProducerStore::new(ProducerStoreLimits {
        records: 2,
        bytes: 2_048,
        batches: 2,
    });
    let mut topic_id = None;
    for (execution, operation_id, value) in [
        (target_execution, target_operation, b"target".as_slice()),
        (foreign_execution, foreign_operation, b"foreign".as_slice()),
    ] {
        let reservation = store
            .reserve(ProducerRecord::new(
                Arc::from("orders"),
                PartitionIndex::from_raw(4),
                10,
                None,
                Some(Bytes::copy_from_slice(value)),
            ))
            .unwrap_or_else(|error| panic!("record reservation failed: {error}"));
        let facts = store
            .commit(reservation)
            .unwrap_or_else(|error| panic!("record commit failed: {error}"));
        store
            .accumulate(execution.batch_id(), operation_id, facts.payload_id())
            .unwrap_or_else(|error| panic!("accumulation failed: {error}"));
        topic_id.get_or_insert(facts.topic_id());
    }
    (
        store,
        topic_id.unwrap_or_else(|| panic!("two records should establish a topic identity")),
    )
}

const fn submission(
    execution: BatchExecutionId,
    operation_id: OperationId,
    topic_id: kafka_client_core::TopicId,
    deadline: Deadline,
) -> ProducerEffect {
    ProducerEffect::SubmitProduce {
        execution,
        deadline_operation_id: operation_id,
        deadline,
        topic_id,
        partition: PartitionIndex::from_raw(4),
        acknowledgements: AcknowledgementPolicy::All,
    }
}
