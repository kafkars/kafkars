//! Exact deadline handoff and corruption-preservation scenarios.

use std::time::Instant;

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, OperationId,
};

use super::SubmissionDeadlines;
use crate::clock::OperationDeadline;

#[test]
fn missing_schedule_preserves_the_active_original_deadline() {
    let execution =
        BatchExecutionId::new(BatchId::from_raw(8), BatchExecutionGeneration::initial());
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(30), Instant::now());
    let mut deadlines = SubmissionDeadlines::new(1);
    deadlines
        .arm(execution, OperationId::from_raw(80), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
    let plan = deadlines
        .plan_handoff(execution)
        .unwrap_or_else(|| panic!("exact deadline should produce a handoff plan"));
    deadlines.remove_handoff_schedule_for_test(execution);

    let Err(returned) = deadlines.commit_handoff(plan) else {
        panic!("missing schedule must return the exact plan")
    };
    assert_eq!(returned.execution(), execution);
    assert_eq!(returned.operation_id(), OperationId::from_raw(80));
    assert_eq!(returned.deadline(), deadline);
    assert_eq!(deadlines.deadline(execution), Some(deadline));
    assert_eq!(deadlines.execution(execution.batch_id()), Some(execution));
    assert_eq!(deadlines.len(), 1);
}

#[test]
fn planning_refuses_a_missing_schedule_without_moving_the_active_owner() {
    let execution =
        BatchExecutionId::new(BatchId::from_raw(9), BatchExecutionGeneration::initial());
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(40), Instant::now());
    let mut deadlines = SubmissionDeadlines::new(1);
    deadlines
        .arm(execution, OperationId::from_raw(90), deadline)
        .unwrap_or_else(|error| panic!("deadline arm failed: {error}"));
    deadlines.remove_handoff_schedule_for_test(execution);

    assert!(deadlines.plan_handoff(execution).is_none());
    assert_eq!(deadlines.deadline(execution), Some(deadline));
    assert_eq!(deadlines.execution(execution.batch_id()), Some(execution));
    assert_eq!(deadlines.len(), 1);
}
