//! Exact prepared-byte and deadline revocation scenarios.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::{
    PreparedExecution, PreparedExecutionError, PreparedExecutionLimits, PreparedRevisionExpectation,
};
use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
    protocol::produce::materialize_explicit_produce_batch,
};

#[test]
fn absent_unarmed_and_armed_revision_phases_are_exact() {
    let execution = execution();
    let mut absent = owner();
    let plan = absent
        .plan_revision(execution, PreparedRevisionExpectation::Absent)
        .unwrap_or_else(|error| panic!("absent preflight failed: {error}"));
    absent.commit_revision(plan);
    assert_eq!(absent.prepared_stats().batches, 0);

    let mut unarmed = owner();
    retain(&mut unarmed, execution);
    let plan = unarmed
        .plan_revision(execution, PreparedRevisionExpectation::Unarmed)
        .unwrap_or_else(|error| panic!("unarmed preflight failed: {error}"));
    unarmed.commit_revision(plan);
    assert_empty(&unarmed);

    let mut armed = owner();
    retain(&mut armed, execution);
    armed
        .arm_for_test(
            execution,
            OperationId::from_raw(9),
            OperationDeadline::from_parts_for_test(Deadline::from_tick(40), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("arm failed: {error}"));
    let plan = armed
        .plan_revision(execution, PreparedRevisionExpectation::Armed)
        .unwrap_or_else(|error| panic!("armed preflight failed: {error}"));
    armed.commit_revision(plan);
    assert_empty(&armed);
    assert_eq!(armed.next_deadline(), None);
}

#[test]
fn phase_or_accounting_mismatch_does_not_remove_prepared_ownership() {
    let execution = execution();
    let mut prepared = owner();
    retain(&mut prepared, execution);
    let before = prepared.prepared_stats();

    assert!(matches!(
        prepared.plan_revision(execution, PreparedRevisionExpectation::Armed),
        Err(PreparedExecutionError::RevisionStateMismatch {
            execution: retained,
            expected: PreparedRevisionExpectation::Armed,
        }) if retained == execution
    ));
    assert_eq!(prepared.prepared_stats(), before);

    let _original = prepared.replace_retained_bytes_for_test(0);
    let corrupt = prepared.prepared_stats();
    assert!(
        prepared
            .plan_revision(execution, PreparedRevisionExpectation::Unarmed)
            .is_err()
    );
    assert_eq!(prepared.prepared_stats(), corrupt);
}

fn owner() -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
        },
    )
}

fn retain(owner: &mut PreparedExecution, execution: BatchExecutionId) {
    let materialized = materialize_explicit_produce_batch(MaterializationBatch::new(
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
    .unwrap_or_else(|error| panic!("encoding failed: {error}"));
    owner
        .retain_for_test(execution, materialized)
        .unwrap_or_else(|error| panic!("retention failed: {error}"));
}

fn assert_empty(owner: &PreparedExecution) {
    assert_eq!(owner.prepared_stats().batches, 0);
    assert_eq!(owner.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(owner.submission_count(), 0);
}

fn execution() -> BatchExecutionId {
    BatchExecutionId::new(BatchId::from_raw(7), BatchExecutionGeneration::initial())
}
