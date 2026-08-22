//! Modern membership start admission and exact public-deadline transfer evidence.

use std::time::Duration;

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    GroupId,
};

use crate::clock::MonotonicClock;

use super::consumer_group_execution::{
    ConsumerGroupExecution, ConsumerGroupExecutionAdmissionError,
};

#[test]
fn first_begin_retains_the_exact_boundary_deadline_and_epoch_zero_effect() {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    let mut execution = ConsumerGroupExecution::new(group_id());
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("consumer begin: {error:?}"));
    let prepared = execution
        .prepared()
        .unwrap_or_else(|| panic!("prepared heartbeat"));
    assert_eq!(prepared.kind(), ConsumerGroupHeartbeatRequestKind::Join);
    assert_eq!(prepared.member_id(), None);
    assert_eq!(prepared.member_epoch(), None);
    assert_eq!(prepared.assignment_generation(), None);
    assert_eq!(prepared.deadline(), capture.operation_deadline());
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Joining
    );
}

#[test]
fn second_begin_cannot_replace_the_accepted_effect() {
    let clock = MonotonicClock::new();
    let mut execution = ConsumerGroupExecution::new(group_id());
    let first_capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(first_capture)
        .unwrap_or_else(|error| panic!("consumer begin: {error:?}"));
    let first = execution.prepared();
    let second_capture = clock
        .capture_deadline_after(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    assert_eq!(
        execution.begin(second_capture),
        Err(ConsumerGroupExecutionAdmissionError::Occupied)
    );
    assert_eq!(execution.prepared(), first);
}

#[test]
fn initial_failure_becomes_one_retained_fatal_without_assignment_loss() {
    let mut execution = ConsumerGroupExecution::new(group_id());
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    execution
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    assert_eq!(
        execution
            .apply_current_failure(ConsumerGroupHeartbeatFailure::Compatibility)
            .unwrap_or_else(|error| panic!("failure: {error:?}")),
        None
    );
    assert_eq!(
        execution.machine().phase(),
        ConsumerGroupHeartbeatPhase::Fatal
    );
    assert_eq!(execution.unsettled(), 0);
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
