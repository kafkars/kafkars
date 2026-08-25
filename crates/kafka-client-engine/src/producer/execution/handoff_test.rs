//! Unified prepared-entry handoff and corruption-preservation scenarios.

use std::time::Instant;

use bytes::Bytes;
use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
    partitioning::TopicMetadataGeneration,
};

use super::{
    PreparedExecution, PreparedExecutionLimits, PreparedProduceError,
    handoff::PreparedProduceHandoffError,
};
use crate::{
    clock::OperationDeadline,
    producer::materialization::{MaterializationBatch, MaterializationRecord},
    protocol::produce::{MaterializedProduce, materialize_explicit_produce_batch},
};

#[test]
fn handoff_preserves_exact_execution_deadline_and_encoded_owner() {
    let execution = execution(7);
    let deadline = deadline(41);
    let materialized = prepared(b"payload");
    let encoded_address = materialized.encoded_records().as_ptr();
    let encoded_length = materialized.encoded_records().len();
    let mut owner = owner();
    retain(&mut owner, execution, deadline, materialized);

    let submission = owner
        .take_driver_submission(execution)
        .unwrap_or_else(|error| panic!("exact handoff failed: {error}"));

    assert_eq!(submission.execution(), execution);
    assert_eq!(submission.deadline(), deadline);
    assert_eq!(owner.prepared_stats().batches, 0);
    assert_eq!(owner.prepared_stats().encoded_record_bytes, 0);
    assert_eq!(owner.submission_count(), 0);
    let (actual_execution, actual_deadline, materialized) = submission.into_parts();
    assert_eq!(actual_execution, execution);
    assert_eq!(actual_deadline, deadline);
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
    assert_eq!(materialized.encoded_records().len(), encoded_length);
}

#[test]
fn replacement_submission_requires_newer_metadata_for_expected_uuid() {
    let replacement = BatchExecutionId::new(
        BatchId::from_raw(7),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("replacement generation")),
    );
    let materialized =
        MaterializedProduce::from_encoded_test_parts("orders", 0, Bytes::from_static(b"encoded"))
            .with_expected_topic_identity(
                Some([7; 16]),
                Some(TopicMetadataGeneration::from_raw(11)),
            );
    let mut submission =
        super::PreparedProduceSubmission::from_test_parts(replacement, deadline(41), materialized);

    assert_eq!(
        submission.retry_topic_identity(),
        Some(([7; 16], TopicMetadataGeneration::from_raw(11)))
    );
    assert!(
        submission.record_retry_topic_identity([7; 16], TopicMetadataGeneration::from_raw(12),)
    );
    assert_eq!(
        submission.retry_topic_identity(),
        Some(([7; 16], TopicMetadataGeneration::from_raw(12)))
    );
    assert!(
        !submission.record_retry_topic_identity([7; 16], TopicMetadataGeneration::from_raw(12),)
    );
}

#[test]
fn stale_execution_rejection_preserves_the_current_entry() {
    let current = execution(9);
    let stale = BatchExecutionId::new(
        current.batch_id(),
        BatchExecutionGeneration::try_from_raw(2)
            .unwrap_or_else(|| panic!("second execution generation must exist")),
    );
    let deadline = deadline(51);
    let materialized = prepared(b"current");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, current, deadline, materialized);

    assert!(matches!(
        owner.take_driver_submission(stale),
        Err(PreparedProduceHandoffError::OwnershipMismatch {
            requested,
            retained: Some(actual),
        }) if requested == stale && actual == current
    ));
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
fn missing_schedule_preserves_the_full_prepared_entry() {
    let execution = execution(13);
    let deadline = deadline(71);
    let materialized = prepared(b"corruption");
    let encoded_address = materialized.encoded_records().as_ptr();
    let encoded_bytes = materialized.retained_record_bytes();
    let mut owner = owner();
    retain(&mut owner, execution, deadline, materialized);
    owner.remove_schedule_for_test(execution);

    assert!(matches!(
        owner.take_driver_submission(execution),
        Err(PreparedProduceHandoffError::ScheduleInconsistent {
            execution: actual,
            deadline: retained,
        }) if actual == execution && retained == deadline
    ));
    assert_eq!(owner.submission_deadline(execution), Some(deadline));
    assert_eq!(owner.submission_count(), 1);
    assert_eq!(owner.prepared_stats().batches, 1);
    assert_eq!(owner.prepared_stats().encoded_record_bytes, encoded_bytes);
    let retained = owner
        .entries
        .get(&execution.batch_id())
        .unwrap_or_else(|| panic!("entry must remain retained"));
    assert_eq!(
        retained.materialized.encoded_records().as_ptr(),
        encoded_address
    );
}

#[test]
fn accounting_corruption_preserves_the_full_prepared_entry() {
    let execution = execution(15);
    let deadline = deadline(81);
    let materialized = prepared(b"accounting");
    let encoded_address = materialized.encoded_records().as_ptr();
    let mut owner = owner();
    retain(&mut owner, execution, deadline, materialized);
    let encoded_bytes = owner.prepared_stats().encoded_record_bytes;
    assert_eq!(owner.replace_retained_bytes_for_test(0), encoded_bytes);

    assert!(matches!(
        owner.take_driver_submission(execution),
        Err(PreparedProduceHandoffError::AccountingInconsistent {
            execution: actual,
            deadline: retained,
            reason: PreparedProduceError::EncodedByteOverflow,
        }) if actual == execution && retained == deadline
    ));
    assert_eq!(owner.submission_deadline(execution), Some(deadline));
    assert_eq!(owner.submission_count(), 1);
    assert_eq!(owner.prepared_stats().batches, 1);

    owner.replace_retained_bytes_for_test(encoded_bytes);
    let (_, _, materialized) = owner
        .take_driver_submission(execution)
        .unwrap_or_else(|error| panic!("repaired handoff failed: {error}"))
        .into_parts();
    assert_eq!(materialized.encoded_records().as_ptr(), encoded_address);
}

const fn execution(value: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(value),
        BatchExecutionGeneration::initial(),
    )
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}

const fn owner() -> PreparedExecution {
    PreparedExecution::new(
        1,
        PreparedExecutionLimits {
            encoded_bytes: 1_024,
            max_batch_bytes: 1_024,
            max_request_bytes: 1_024,
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
        .retain_for_test(execution, materialized)
        .unwrap_or_else(|error| panic!("prepared insertion failed: {error}"));
    owner
        .arm_for_test(execution, OperationId::from_raw(12), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
}

fn prepared(value: &'static [u8]) -> MaterializedProduce {
    let batch = MaterializationBatch::try_for_test(
        "orders".to_owned(),
        4,
        vec![MaterializationRecord::new(
            100,
            None,
            Some(Bytes::from_static(value)),
            Vec::new(),
        )],
        usize::MAX,
    )
    .unwrap_or_else(|| panic!("test materialization batch must be representable"));
    materialize_explicit_produce_batch(batch)
        .unwrap_or_else(|error| panic!("test materialization failed: {error}"))
}
