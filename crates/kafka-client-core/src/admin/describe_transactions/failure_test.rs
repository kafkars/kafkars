//! Deadline, delivery-certainty, and invalid-state scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeTransactionDescription, AdminDescribeTransactionOutcome,
    AdminDescribeTransactionsEffect, AdminDescribeTransactionsFailureKind,
    AdminDescribeTransactionsInput, AdminDescribeTransactionsMachine,
    AdminDescribeTransactionsMachineError, AdminDescribeTransactionsPlan,
    AdminDescribeTransactionsTerminal, AdminDescribeTransactionsTransition,
};

#[test]
fn original_deadline_and_driver_certainty_settle_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AdminDescribeTransactionsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start: {error}")),
        AdminDescribeTransactionsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    start(&mut rejected);
    assert_failure(
        rejected
            .apply(AdminDescribeTransactionsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection: {error}")),
        AdminDescribeTransactionsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut submitted = machine(20);
    start(&mut submitted);
    submitted
        .apply(AdminDescribeTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert_failure(
        submitted
            .apply(AdminDescribeTransactionsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            })
            .unwrap_or_else(|error| panic!("transport terminal: {error}")),
        AdminDescribeTransactionsFailureKind::Transport,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn any_completed_coordinator_call_makes_later_failure_possibly_sent() {
    let mut machine = machine(20);
    start(&mut machine);
    machine
        .apply(AdminDescribeTransactionsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminDescribeTransactionsInput::BrokerResponded {
                throttle_time_ms: 0,
                outcome: described("invoice-worker"),
            })
        })
        .unwrap_or_else(|error| panic!("settle first ID: {error}"));

    assert_failure(
        machine
            .apply(AdminDescribeTransactionsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("reject second ID: {error}")),
        AdminDescribeTransactionsFailureKind::DriverRejected,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn facts_are_rejected_outside_their_explicit_ownership_stage() {
    let mut machine = machine(20);
    assert_eq!(
        machine.apply(AdminDescribeTransactionsInput::DriverAccepted),
        Err(AdminDescribeTransactionsMachineError::InvalidState)
    );
    start(&mut machine);
    assert_eq!(
        machine.apply(AdminDescribeTransactionsInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(AdminDescribeTransactionsMachineError::InvalidState)
    );
}

fn machine(deadline: u64) -> AdminDescribeTransactionsMachine {
    AdminDescribeTransactionsMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(deadline),
        AdminDescribeTransactionsPlan::new(vec![
            "invoice-worker".to_owned(),
            "audit-writer".to_owned(),
        ])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn described(transactional_id: &str) -> AdminDescribeTransactionOutcome {
    AdminDescribeTransactionOutcome::described(
        transactional_id.to_owned(),
        AdminDescribeTransactionDescription::new("Empty".to_owned(), -1, None, -1, -1, Vec::new()),
    )
}

fn start(machine: &mut AdminDescribeTransactionsMachine) {
    let transition = machine
        .apply(AdminDescribeTransactionsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(AdminDescribeTransactionsEffect::Submit { .. })
    ));
}

fn assert_failure(
    transition: AdminDescribeTransactionsTransition,
    kind: AdminDescribeTransactionsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminDescribeTransactionsEffect::Complete {
        terminal: AdminDescribeTransactionsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
