//! Deadline, delivery-certainty, and invalid-state scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeProducerOutcome, AdminDescribeProducerTarget, AdminDescribeProducersEffect,
    AdminDescribeProducersFailureKind, AdminDescribeProducersInput, AdminDescribeProducersMachine,
    AdminDescribeProducersMachineError, AdminDescribeProducersPlan, AdminDescribeProducersTerminal,
    AdminDescribeProducersTransition,
};

#[test]
fn original_deadline_and_driver_certainty_settle_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AdminDescribeProducersInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start: {error}")),
        AdminDescribeProducersFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    start(&mut rejected);
    assert_failure(
        rejected
            .apply(AdminDescribeProducersInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection: {error}")),
        AdminDescribeProducersFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut submitted = machine(20);
    start(&mut submitted);
    submitted
        .apply(AdminDescribeProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert_failure(
        submitted
            .apply(AdminDescribeProducersInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            })
            .unwrap_or_else(|error| panic!("transport terminal: {error}")),
        AdminDescribeProducersFailureKind::Transport,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn any_completed_leader_call_makes_later_whole_operation_failure_possibly_sent() {
    let mut machine = machine(20);
    start(&mut machine);
    machine
        .apply(AdminDescribeProducersInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminDescribeProducersInput::BrokerResponded {
                throttle_time_ms: 0,
                outcome: AdminDescribeProducerOutcome::described(
                    "orders".to_owned(),
                    2,
                    Vec::new(),
                ),
            })
        })
        .unwrap_or_else(|error| panic!("settle first target: {error}"));

    assert_failure(
        machine
            .apply(AdminDescribeProducersInput::DriverRejected)
            .unwrap_or_else(|error| panic!("reject second target: {error}")),
        AdminDescribeProducersFailureKind::DriverRejected,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn facts_are_rejected_outside_their_explicit_ownership_stage() {
    let mut machine = machine(20);
    assert_eq!(
        machine.apply(AdminDescribeProducersInput::DriverAccepted),
        Err(AdminDescribeProducersMachineError::InvalidState)
    );
    start(&mut machine);
    assert_eq!(
        machine.apply(AdminDescribeProducersInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(AdminDescribeProducersMachineError::InvalidState)
    );
}

fn machine(deadline: u64) -> AdminDescribeProducersMachine {
    AdminDescribeProducersMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        AdminDescribeProducersPlan::new(
            vec![
                AdminDescribeProducerTarget::new("orders".to_owned(), 2),
                AdminDescribeProducerTarget::new("audit".to_owned(), 0),
            ],
            None,
        )
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn start(machine: &mut AdminDescribeProducersMachine) {
    let transition = machine
        .apply(AdminDescribeProducersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(AdminDescribeProducersEffect::Submit { .. })
    ));
}

fn assert_failure(
    transition: AdminDescribeProducersTransition,
    kind: AdminDescribeProducersFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminDescribeProducersEffect::Complete {
        terminal: AdminDescribeProducersTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
