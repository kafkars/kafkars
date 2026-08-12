//! Unified deadline ordering, replay, expiry, and generation-fencing scenarios.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, Moment, OperationId,
    ProducerInput,
};

use super::{PreparedExecution, PreparedExecutionLimits, SubmissionDeadlineError};
use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
    protocol::produce::materialize_explicit_produce_batch,
};

#[test]
fn due_entries_emit_exact_facts_in_deadline_then_batch_order() {
    let mut owner = owner(4);
    for (batch, operation, due_at) in [(9, 90, 40), (2, 20, 40), (7, 70, 10), (1, 10, 60)] {
        retain_and_arm(&mut owner, batch, operation, due_at);
    }
    let retained_bytes = owner.prepared_stats().encoded_record_bytes;

    assert_eq!(
        owner.drain_due(Moment::from_tick(40), usize::MAX),
        [
            ProducerInput::DeadlineElapsed {
                operation_id: OperationId::from_raw(70),
                now: Moment::from_tick(40),
            },
            ProducerInput::DeadlineElapsed {
                operation_id: OperationId::from_raw(20),
                now: Moment::from_tick(40),
            },
            ProducerInput::DeadlineElapsed {
                operation_id: OperationId::from_raw(90),
                now: Moment::from_tick(40),
            },
        ]
    );
    assert_eq!(owner.submission_count(), 1);
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(60)));
    assert_eq!(owner.prepared_stats().batches, 4);
    assert_eq!(
        owner.prepared_stats().encoded_record_bytes,
        retained_bytes,
        "deadline expiry must retain encoded bytes until core releases the batch"
    );
}

#[test]
fn exact_arm_replay_is_idempotent_and_conflict_preserves_original_facts() {
    let mut owner = owner(1);
    let execution = execution(1);
    let original = deadline(30);
    owner
        .retain_for_test(execution, prepared(1))
        .unwrap_or_else(|error| panic!("retention failed: {error}"));

    assert_eq!(
        owner.arm_for_test(execution, OperationId::from_raw(10), original),
        Ok(true)
    );
    assert_eq!(
        owner.arm_for_test(execution, OperationId::from_raw(10), original),
        Ok(false)
    );
    assert_eq!(
        owner.arm_for_test(execution, OperationId::from_raw(11), original),
        Err(SubmissionDeadlineError::ConflictingBatch {
            batch_id: BatchId::from_raw(1),
        })
    );
    assert_eq!(owner.submission_deadline(execution), Some(original));
    assert_eq!(owner.submission_count(), 1);
}

#[test]
fn stale_generation_cannot_arm_or_take_the_current_entry() {
    let mut owner = owner(1);
    let current = execution(8);
    let stale = BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second generation must exist")),
    );
    let current_deadline = deadline(12);
    owner
        .retain_for_test(current, prepared(8))
        .unwrap_or_else(|error| panic!("retention failed: {error}"));
    owner
        .arm_for_test(current, OperationId::from_raw(80), current_deadline)
        .unwrap_or_else(|error| panic!("current arm failed: {error}"));
    let before = owner.prepared_stats();

    assert_eq!(
        owner.arm_for_test(stale, OperationId::from_raw(81), deadline(13)),
        Err(SubmissionDeadlineError::ConflictingBatch {
            batch_id: current.batch_id(),
        })
    );
    assert!(owner.take_driver_submission(stale).is_err());
    assert_eq!(owner.prepared_stats(), before);
    assert_eq!(owner.submission_deadline(current), Some(current_deadline));
}

const fn owner(capacity: usize) -> PreparedExecution {
    PreparedExecution::new(
        capacity,
        PreparedExecutionLimits {
            encoded_bytes: usize::MAX,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
        },
    )
}

fn retain_and_arm(owner: &mut PreparedExecution, batch: u64, operation: u64, due_at: u64) {
    let execution = execution(batch);
    owner
        .retain_for_test(execution, prepared(batch))
        .unwrap_or_else(|error| panic!("retention failed: {error}"));
    owner
        .arm_for_test(
            execution,
            OperationId::from_raw(operation),
            deadline(due_at),
        )
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
}

const fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

fn prepared(value: u64) -> crate::protocol::produce::MaterializedProduce {
    let batch = MaterializationBatch::try_for_test(
        "orders".to_owned(),
        i32::try_from(value).unwrap_or_else(|_| panic!("small partition must fit")),
        vec![MaterializationRecord::new(
            100,
            None,
            Some(Bytes::from_static(b"value")),
            Vec::new(),
        )],
        usize::MAX,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("test materialization failed: {error}"))
}
