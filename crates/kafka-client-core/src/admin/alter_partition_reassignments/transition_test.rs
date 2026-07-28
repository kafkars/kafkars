//! Single-attempt and terminal-correlation reassignment scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterPartitionReassignment, AlterPartitionReassignmentOutcome,
    AlterPartitionReassignmentsBatch, AlterPartitionReassignmentsEffect,
    AlterPartitionReassignmentsFailureKind, AlterPartitionReassignmentsInput,
    AlterPartitionReassignmentsMachine, AlterPartitionReassignmentsPlan,
    AlterPartitionReassignmentsState, AlterPartitionReassignmentsTerminal,
    PartitionReassignmentTarget,
};

#[test]
fn accepted_work_submits_once_with_original_deadline_and_plan() {
    let plan = plan();
    let deadline = Deadline::from_tick(50);
    let mut machine =
        AlterPartitionReassignmentsMachine::new(OperationId::from_raw(7), deadline, plan.clone());

    let transition = machine
        .apply(AlterPartitionReassignmentsInput::Start {
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert_eq!(
        transition.into_effect(),
        Some(AlterPartitionReassignmentsEffect::Submit {
            operation_id: OperationId::from_raw(7),
            deadline,
            plan,
        })
    );
    assert_eq!(
        machine.state(),
        AlterPartitionReassignmentsState::AwaitingDriver
    );
}

#[test]
fn elapsed_start_is_definitely_unsent_and_terminal_once() {
    let mut machine = AlterPartitionReassignmentsMachine::new(
        OperationId::from_raw(3),
        Deadline::from_tick(9),
        plan(),
    );
    let transition = machine
        .apply(AlterPartitionReassignmentsInput::Start {
            now: Moment::from_tick(9),
        })
        .unwrap_or_else(|error| panic!("elapsed start: {error}"));
    let Some(AlterPartitionReassignmentsEffect::Complete { terminal, .. }) =
        transition.into_effect()
    else {
        panic!("expected terminal");
    };
    let AlterPartitionReassignmentsTerminal::Failed(failure) = terminal else {
        panic!("expected failure");
    };
    assert_eq!(
        failure.kind(),
        &AlterPartitionReassignmentsFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert!(
        machine
            .apply(AlterPartitionReassignmentsInput::DriverAccepted)
            .is_err()
    );
}

#[test]
fn response_identity_mismatch_cannot_bind_to_requested_partition() {
    let mut machine = submitted();
    let transition = machine
        .apply(AlterPartitionReassignmentsInput::BrokerResponded {
            batch: AlterPartitionReassignmentsBatch::new(
                0,
                vec![AlterPartitionReassignmentOutcome::altered(
                    "orders".to_owned(),
                    9,
                )],
            ),
        })
        .unwrap_or_else(|error| panic!("terminal: {error}"));
    let Some(AlterPartitionReassignmentsEffect::Complete { terminal, .. }) =
        transition.into_effect()
    else {
        panic!("expected terminal");
    };
    let AlterPartitionReassignmentsTerminal::Failed(failure) = terminal else {
        panic!("expected correlation failure");
    };
    assert_eq!(
        failure.kind(),
        &AlterPartitionReassignmentsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

fn submitted() -> AlterPartitionReassignmentsMachine {
    let mut machine = AlterPartitionReassignmentsMachine::new(
        OperationId::from_raw(1),
        Deadline::from_tick(50),
        plan(),
    );
    machine
        .apply(AlterPartitionReassignmentsInput::Start {
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(AlterPartitionReassignmentsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    machine
}

fn plan() -> AlterPartitionReassignmentsPlan {
    AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
        "orders".to_owned(),
        0,
        PartitionReassignmentTarget::Replicas(vec![1, 2]),
    )])
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
