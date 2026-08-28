//! Stable prepared-submission selection and linear transfer scenarios.

use std::{sync::OnceLock, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::{
    PreparedExecution, PreparedExecutionLimits, PreparedProduceHandoffError,
    PreparedProduceSubmission,
};
use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

#[test]
fn next_submission_uses_lowest_batch_id_as_core_admission_order() {
    let mut owner = candidate_owner(3, 1_024);
    // Arming out of order proves selection follows core's monotonic BatchId.
    for (batch, topic, partition) in [(9, "newest", 9), (3, "oldest", 3), (7, "middle", 7)] {
        retain(&mut owner, batch, topic, partition, b"encoded");
    }

    for (batch, topic, partition) in [(3, "oldest", 3), (7, "middle", 7), (9, "newest", 9)] {
        let submission = owner
            .take_next_driver_submission()
            .unwrap_or_else(|error| panic!("ordered handoff failed: {error}"))
            .unwrap_or_else(|| panic!("armed submission should be ready"));
        assert_eq!(submission.execution(), execution(batch));
        let (_, _, materialized) = submission.into_parts();
        assert_eq!(materialized.topic_name(), topic);
        assert_eq!(materialized.partition(), partition);
    }
    assert!(
        owner
            .take_next_driver_submission()
            .is_ok_and(|next| next.is_none())
    );
    assert_eq!(owner.prepared_stats().batches, 0);
}

#[test]
fn materialized_but_unarmed_batch_is_not_a_candidate() {
    let mut owner = candidate_owner(2, 1_024);
    owner
        .retain_for_test(execution(1), materialized("waiting", 1, b"encoded"))
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    retain(&mut owner, 2, "ready", 2, b"encoded");

    assert_eq!(take_ids(&mut owner), vec![execution(2)]);
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.submission_count(), 0);
}

#[test]
fn corrupt_group_accounting_rejects_before_any_entry_is_detached() {
    let mut owner = candidate_owner(2, 1_024);
    retain(&mut owner, 1, "orders", 0, b"123456");
    retain(&mut owner, 2, "orders", 1, b"12345");
    owner.replace_retained_bytes_for_test(6);
    let before = owner.prepared_stats();

    assert!(matches!(
        owner.take_next_driver_submissions(),
        Err(PreparedProduceHandoffError::AccountingInconsistent { .. })
    ));
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_count(), 2);
    assert!(owner.next_deadline().is_some());
}

#[test]
fn oversized_first_candidate_is_rejected_without_detaching_ownership() {
    let mut owner = candidate_owner(1, 5);
    retain(&mut owner, 1, "orders", 0, b"123456");

    assert!(matches!(
        owner.take_next_driver_submissions(),
        Err(PreparedProduceHandoffError::RequestByteLimit {
            execution: actual,
            encoded_bytes: 6,
            limit: 5,
        }) if actual == execution(1)
    ));
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.submission_count(), 1);
}

fn candidate_owner(capacity: usize, max_request_bytes: usize) -> PreparedExecution {
    PreparedExecution::new(
        capacity,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes,
        },
    )
}

fn retain(
    owner: &mut PreparedExecution,
    batch: u64,
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
) {
    retain_at(
        owner,
        execution(batch),
        topic,
        partition,
        records,
        shared_deadline(),
    );
}

fn retain_at(
    owner: &mut PreparedExecution,
    execution: BatchExecutionId,
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
    deadline: OperationDeadline,
) {
    retain_materialized(
        owner,
        execution,
        materialized(topic, partition, records),
        deadline,
    );
}

fn retain_materialized(
    owner: &mut PreparedExecution,
    execution: BatchExecutionId,
    materialized: MaterializedProduce,
    deadline: OperationDeadline,
) {
    owner
        .retain_for_test(execution, materialized)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    owner
        .arm_for_test(
            execution,
            OperationId::from_raw(execution.batch_id().get()),
            deadline,
        )
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
}

fn take_ids(owner: &mut PreparedExecution) -> Vec<BatchExecutionId> {
    owner
        .take_next_driver_submissions()
        .unwrap_or_else(|error| panic!("candidate-window handoff: {error}"))
        .iter()
        .map(PreparedProduceSubmission::execution)
        .collect()
}

fn shared_deadline() -> OperationDeadline {
    static TRANSPORT: OnceLock<Instant> = OnceLock::new();
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(100),
        *TRANSPORT.get_or_init(Instant::now),
    )
}

const fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn materialized(
    topic: &'static str,
    partition: i32,
    records: &'static [u8],
) -> MaterializedProduce {
    MaterializedProduce::from_encoded_test_parts(topic, partition, Bytes::from_static(records))
}
