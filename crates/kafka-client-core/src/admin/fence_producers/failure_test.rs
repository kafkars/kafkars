//! Deadline, delivery-certainty, and invalid-state scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminFenceProducerOutcome, AdminFenceProducersEffect, AdminFenceProducersFailureKind,
    AdminFenceProducersInput, AdminFenceProducersMachine, AdminFenceProducersMachineError,
    AdminFenceProducersPlan, AdminFenceProducersTerminal, AdminFenceProducersTransition,
    AdminFencedProducerIdentity,
};

#[test]
fn original_deadline_and_driver_certainty_settle_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AdminFenceProducersInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start: {error}")),
        AdminFenceProducersFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    start(&mut rejected);
    assert_failure(
        rejected
            .apply(AdminFenceProducersInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection: {error}")),
        AdminFenceProducersFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut submitted = machine(20);
    start(&mut submitted);
    submitted
        .apply(AdminFenceProducersInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert_failure(
        submitted
            .apply(AdminFenceProducersInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            })
            .unwrap_or_else(|error| panic!("transport terminal: {error}")),
        AdminFenceProducersFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn any_completed_coordinator_call_makes_later_failure_possibly_sent() {
    let mut machine = machine(20);
    start(&mut machine);
    machine
        .apply(AdminFenceProducersInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminFenceProducersInput::BrokerResponded {
                throttle_time_ms: 0,
                outcome: fenced("invoice-worker", 91, 7),
            })
        })
        .unwrap_or_else(|error| panic!("settle first ID: {error}"));

    assert_failure(
        machine
            .apply(AdminFenceProducersInput::DriverRejected)
            .unwrap_or_else(|error| panic!("reject second ID: {error}")),
        AdminFenceProducersFailureKind::DriverRejected,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn facts_are_rejected_outside_their_explicit_ownership_stage() {
    let mut machine = machine(20);
    assert_eq!(
        machine.apply(AdminFenceProducersInput::DriverAccepted),
        Err(AdminFenceProducersMachineError::InvalidState)
    );
    start(&mut machine);
    assert_eq!(
        machine.apply(AdminFenceProducersInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(AdminFenceProducersMachineError::InvalidState)
    );
}

fn machine(deadline: u64) -> AdminFenceProducersMachine {
    AdminFenceProducersMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(deadline),
        AdminFenceProducersPlan::new(vec!["invoice-worker".to_owned(), "audit-writer".to_owned()])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn fenced(transactional_id: &str, producer_id: i64, epoch: i16) -> AdminFenceProducerOutcome {
    let identity = AdminFencedProducerIdentity::try_new(producer_id, epoch)
        .unwrap_or_else(|| panic!("valid producer identity"));
    AdminFenceProducerOutcome::fenced(transactional_id.to_owned(), identity)
}

fn start(machine: &mut AdminFenceProducersMachine) {
    let transition = machine
        .apply(AdminFenceProducersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(AdminFenceProducersEffect::Submit { .. })
    ));
}

fn assert_failure(
    transition: AdminFenceProducersTransition,
    kind: AdminFenceProducersFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminFenceProducersEffect::Complete {
        terminal: AdminFenceProducersTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
