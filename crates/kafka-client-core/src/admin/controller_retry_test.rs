//! Controller retry preserves the original creation plan, deadline, and sole completion.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CreateTopicSpecification, CreateTopicsEffect, CreateTopicsFailureKind, CreateTopicsInput,
    CreateTopicsMachine, CreateTopicsMachineError, CreateTopicsPlan, CreateTopicsState,
    CreateTopicsTerminal,
};

fn accepted() -> CreateTopicsMachine {
    let plan = CreateTopicsPlan::new(
        vec![CreateTopicSpecification::new("orders", 3, 2, Vec::new())],
        false,
    )
    .unwrap_or_else(|error| panic!("valid creation plan: {error}"));
    let mut machine =
        CreateTopicsMachine::new(OperationId::from_raw(7), Deadline::from_tick(10), plan);
    machine
        .apply(CreateTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(CreateTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    machine
}

#[test]
fn controller_retry_is_once_and_reuses_exact_plan_deadline_and_identity() {
    let mut machine = accepted();
    let effect = machine
        .apply(CreateTopicsInput::ControllerRouteUnavailable {
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("controller retry: {error}"))
        .into_effect();
    let Some(CreateTopicsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = effect
    else {
        panic!("one resubmission required");
    };
    assert_eq!(operation_id, OperationId::from_raw(7));
    assert_eq!(deadline, Deadline::from_tick(10));
    assert_eq!(plan.topics()[0].name(), "orders");
    assert_eq!(machine.state(), CreateTopicsState::AwaitingDriver);
    machine
        .apply(CreateTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("retry acceptance: {error}"));
    assert_failure(&mut machine, 3, CreateTopicsFailureKind::Transport);
}

#[test]
fn expired_controller_refresh_cannot_restart_the_public_timeout() {
    assert_failure(
        &mut accepted(),
        10,
        CreateTopicsFailureKind::DeadlineElapsed,
    );
}

fn assert_failure(machine: &mut CreateTopicsMachine, now: u64, kind: CreateTopicsFailureKind) {
    let transition = machine
        .apply(CreateTopicsInput::ControllerRouteUnavailable {
            now: Moment::from_tick(now),
        })
        .unwrap_or_else(|error| panic!("settle controller failure: {error}"));
    let Some(CreateTopicsEffect::Complete {
        terminal: CreateTopicsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("exactly one failure required");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert_eq!(machine.state(), CreateTopicsState::Completed);
    assert_eq!(
        machine.apply(CreateTopicsInput::ControllerRouteUnavailable {
            now: Moment::from_tick(now)
        }),
        Err(CreateTopicsMachineError::AlreadyCompleted)
    );
}

#[test]
fn possibly_sent_transport_failure_remains_terminal_without_retry() {
    let mut machine = accepted();
    let transition = machine
        .apply(CreateTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("uncertain transport: {error}"));
    let Some(CreateTopicsEffect::Complete {
        terminal: CreateTopicsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("uncertain mutation must not replay");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}
