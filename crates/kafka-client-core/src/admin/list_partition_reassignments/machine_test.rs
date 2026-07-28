//! Deterministic lifecycle tests for partition-reassignment listing.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual failure messages"
)]

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsBatch,
    ListPartitionReassignmentsEffect, ListPartitionReassignmentsFailureKind,
    ListPartitionReassignmentsInput, ListPartitionReassignmentsMachine,
    ListPartitionReassignmentsPlan, ListPartitionReassignmentsState,
    ListPartitionReassignmentsTerminal, PartitionReassignment, PartitionReassignmentOutcome,
};

fn selected_machine() -> ListPartitionReassignmentsMachine {
    ListPartitionReassignmentsMachine::new(
        OperationId::from_raw(7),
        Deadline::from_tick(20),
        ListPartitionReassignmentsPlan::selected(vec![
            ListPartitionReassignmentTarget::new("z".to_owned(), 2),
            ListPartitionReassignmentTarget::new("a".to_owned(), 0),
        ])
        .expect("valid selection"),
    )
}

fn outcome(topic: &str, partition: i32) -> PartitionReassignmentOutcome {
    PartitionReassignmentOutcome::new(
        topic.to_owned(),
        partition,
        PartitionReassignment::new(vec![1, 2], vec![2], vec![]),
    )
}

#[test]
fn selected_response_is_a_caller_ordered_subsequence() {
    let mut machine = selected_machine();
    let submit = machine
        .apply(ListPartitionReassignmentsInput::Start {
            now: Moment::from_tick(1),
        })
        .expect("start")
        .into_effect();
    assert!(matches!(
        submit,
        Some(ListPartitionReassignmentsEffect::Submit { .. })
    ));
    machine
        .apply(ListPartitionReassignmentsInput::DriverAccepted)
        .expect("accepted");
    let complete = machine
        .apply(ListPartitionReassignmentsInput::BrokerResponded {
            batch: ListPartitionReassignmentsBatch::new(3, vec![outcome("a", 0)]),
        })
        .expect("response")
        .into_effect();
    assert!(matches!(
        complete,
        Some(ListPartitionReassignmentsEffect::Complete {
            terminal: ListPartitionReassignmentsTerminal::Reassignments(_),
            ..
        })
    ));
    assert_eq!(machine.state(), ListPartitionReassignmentsState::Completed);
}

#[test]
fn selected_response_rejects_broker_order_instead_of_rebinding_results() {
    let mut machine = selected_machine();
    machine
        .apply(ListPartitionReassignmentsInput::Start {
            now: Moment::from_tick(1),
        })
        .expect("start");
    machine
        .apply(ListPartitionReassignmentsInput::DriverAccepted)
        .expect("accepted");
    let complete = machine
        .apply(ListPartitionReassignmentsInput::BrokerResponded {
            batch: ListPartitionReassignmentsBatch::new(0, vec![outcome("a", 0), outcome("z", 2)]),
        })
        .expect("terminal")
        .into_effect();
    assert!(matches!(
        complete,
        Some(ListPartitionReassignmentsEffect::Complete {
            terminal: ListPartitionReassignmentsTerminal::Failed(failure),
            ..
        }) if failure.kind() == &ListPartitionReassignmentsFailureKind::InvalidResponse
            && failure.delivery() == DeliveryStatus::PossiblySent
    ));
}

#[test]
fn elapsed_public_deadline_completes_without_submission() {
    let mut machine = selected_machine();
    let complete = machine
        .apply(ListPartitionReassignmentsInput::Start {
            now: Moment::from_tick(20),
        })
        .expect("deadline terminal")
        .into_effect();
    assert!(matches!(
        complete,
        Some(ListPartitionReassignmentsEffect::Complete {
            terminal: ListPartitionReassignmentsTerminal::Failed(failure),
            ..
        }) if failure.kind() == &ListPartitionReassignmentsFailureKind::DeadlineElapsed
            && failure.delivery() == DeliveryStatus::NotSent
    ));
}
