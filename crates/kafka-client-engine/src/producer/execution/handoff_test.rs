//! Exact prepared-request and original-deadline handoff scenarios.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::{PreparedExecution, PreparedExecutionLimits, handoff::PreparedProduceHandoffError};
use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
    protocol::produce::{MaterializedProduce, materialize_explicit_produce_batch},
};

#[test]
fn handoff_preserves_exact_execution_deadline_and_encoded_owner() {
    let execution_id = execution(7);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(41), Instant::now());
    let materialized = prepared(b"payload");
    let encoded_address = materialized.encoded_records().as_ptr();
    let encoded_length = materialized.encoded_records().len();
    let mut owner = owner();
    retain(&mut owner, execution_id, deadline, materialized);

    let submission = owner
        .take_driver_submission(execution_id)
        .unwrap_or_else(|error| panic!("exact handoff failed: {error}"));

    assert_eq!(submission.execution(), execution_id);
    assert_eq!(submission.deadline(), deadline);
    assert_eq!(owner.prepared_stats().batches, 0);
    assert_eq!(owner.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(owner.submission_count(), 0);
    let (actual_execution, actual_deadline, materialized) = submission.into_parts();
    assert_eq!(actual_execution, execution_id);
    assert_eq!(actual_deadline, deadline);
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
    assert_eq!(materialized.encoded_records().len(), encoded_length);

    let request = materialized.into_name_routed_request(300);
    let records = request.topic_data[0].partition_data[0]
        .records
        .as_ref()
        .unwrap_or_else(|| panic!("materialized request must retain encoded records"));
    assert_eq!(records.as_ptr(), encoded_address);
    assert_eq!(records.len(), encoded_length);
    assert_eq!(request.acks, -1);
    assert_eq!(request.timeout_ms, 300);
}

#[test]
fn stale_execution_rejection_leaves_both_exact_owners_unchanged() {
    let current = execution(9);
    let stale = BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second execution generation must exist")),
    );
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(51), Instant::now());
    let materialized = prepared(b"current");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, current, deadline, materialized);

    let Err(PreparedProduceHandoffError::OwnershipMismatch {
        requested,
        prepared,
        deadline: retained_deadline,
    }) = owner.take_driver_submission(stale)
    else {
        panic!("stale execution must preserve both current owners")
    };
    assert_eq!(requested, stale);
    assert_eq!(prepared, Some(current));
    assert_eq!(retained_deadline, Some(current));
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.submission_count(), 1);

    let (_, actual_deadline, materialized) = owner
        .take_driver_submission(current)
        .unwrap_or_else(|error| panic!("current handoff failed: {error}"))
        .into_parts();
    assert_eq!(actual_deadline, deadline);
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
}

#[test]
fn one_sided_ownership_rejection_never_consumes_the_present_owner() {
    let execution_id = execution(11);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(61), Instant::now());
    let materialized = prepared(b"one-sided");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    owner
        .prepared
        .insert(execution_id, materialized)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));

    assert!(matches!(
        owner.take_driver_submission(execution_id),
        Err(PreparedProduceHandoffError::OwnershipMismatch {
            requested,
            prepared: Some(retained),
            deadline: None,
        }) if requested == execution_id && retained == execution_id
    ));
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.submission_count(), 0);

    owner
        .deadlines
        .arm(execution_id, OperationId::from_raw(12), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
    let (_, actual_deadline, materialized) = owner
        .take_driver_submission(execution_id)
        .unwrap_or_else(|error| panic!("paired handoff failed: {error}"))
        .into_parts();
    assert_eq!(actual_deadline, deadline);
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
}

#[test]
fn deadline_corruption_returns_facts_and_preserves_exact_prepared_bytes() {
    let execution_id = execution(13);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(71), Instant::now());
    let materialized = prepared(b"corruption");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, execution_id, deadline, materialized);
    let plan = owner
        .plan_driver_submission(execution_id)
        .unwrap_or_else(|error| panic!("exact owners should produce a plan: {error}"));
    owner
        .deadlines
        .remove_handoff_schedule_for_test(execution_id);

    assert!(matches!(
        owner.commit_driver_submission(plan),
        Err(PreparedProduceHandoffError::DeadlineInconsistent {
            requested,
            prepared: Some(prepared),
            active: Some(active),
            deadline: retained,
            plan: Some(returned),
        }) if requested == execution_id
            && prepared == execution_id
            && active == execution_id
            && retained == deadline
            && returned.execution() == execution_id
            && returned.operation_id() == OperationId::from_raw(12)
            && returned.deadline() == deadline
    ));
    assert_eq!(owner.submission_deadline(execution_id), Some(deadline));
    assert_eq!(owner.submission_count(), 1);
    assert_eq!(owner.prepared_stats().batches, 1);
    let preserved = owner
        .prepared
        .take(execution_id)
        .unwrap_or_else(|error| panic!("preserved prepared request missing: {error}"));
    assert_eq!(preserved.encoded_records().as_ptr(), encoded_address);
}

#[test]
fn prepared_preflight_corruption_preserves_both_exact_owners() {
    let execution_id = execution(15);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(81), Instant::now());
    let materialized = prepared(b"preflight");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, execution_id, deadline, materialized);
    let encoded_bytes = owner.prepared_stats().encoded_record_bytes;
    assert_eq!(
        owner.prepared.replace_retained_bytes_for_handoff_test(0),
        encoded_bytes
    );

    assert!(matches!(
        owner.take_driver_submission(execution_id),
        Err(PreparedProduceHandoffError::PreparedPreflightInconsistent {
            execution,
            deadline: retained,
            reason: crate::producer::prepared::PreparedProduceError::EncodedByteOverflow,
        }) if execution == execution_id && retained == deadline
    ));
    assert_eq!(owner.submission_deadline(execution_id), Some(deadline));
    assert_eq!(owner.submission_count(), 1);
    assert_eq!(owner.prepared_stats().batches, 1);
    owner
        .prepared
        .replace_retained_bytes_for_handoff_test(encoded_bytes);
    let (_, actual_deadline, materialized) = owner
        .take_driver_submission(execution_id)
        .unwrap_or_else(|error| panic!("repaired handoff failed: {error}"))
        .into_parts();
    assert_eq!(actual_deadline, deadline);
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
}

#[test]
fn post_plan_execution_drift_reports_the_actual_prepared_owner() {
    let requested = execution(17);
    let replacement = BatchExecutionId::new(
        requested.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second execution generation must exist")),
    );
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(91), Instant::now());
    let materialized = prepared(b"post-plan");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, requested, deadline, materialized);
    let plan = owner
        .plan_driver_submission(requested)
        .unwrap_or_else(|error| panic!("exact owners should produce a plan: {error}"));
    assert_eq!(
        owner
            .prepared
            .replace_execution_for_handoff_test(requested.batch_id(), replacement),
        Some(requested)
    );

    assert!(matches!(
        owner.commit_driver_submission(plan),
        Err(PreparedProduceHandoffError::PreparedCommitInconsistent {
            requested: actual,
            deadline: detached,
            prepared: Some(retained),
            reason: crate::producer::prepared::PreparedProduceError::ExecutionMismatch,
        }) if actual == requested && detached == deadline && retained == replacement
    ));
    assert_eq!(owner.submission_deadline(requested), None);
    assert_eq!(owner.submission_count(), 0);
    assert_eq!(owner.prepared_stats().batches, 1);
    let preserved = owner
        .prepared
        .take(replacement)
        .unwrap_or_else(|error| panic!("replacement owner missing: {error}"));
    assert_eq!(preserved.encoded_records().as_ptr(), encoded_address);
}

const fn execution(value: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(value),
        BatchExecutionGeneration::initial(),
    )
}

const fn owner() -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
        },
    )
}

fn retain(
    owner: &mut PreparedExecution,
    execution: BatchExecutionId,
    deadline: OperationDeadline,
    materialized: MaterializedProduce,
) {
    owner
        .prepared
        .insert(execution, materialized)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    owner
        .deadlines
        .arm(execution, OperationId::from_raw(12), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
}

fn prepared(value: &'static [u8]) -> MaterializedProduce {
    materialize_explicit_produce_batch(MaterializationBatch::new(
        "orders".to_owned(),
        4,
        vec![MaterializationRecord::new(
            100,
            None,
            Some(Bytes::from_static(value)),
            Vec::new(),
        )],
        usize::MAX,
    ))
    .unwrap_or_else(|error| panic!("test materialization failed: {error}"))
}
