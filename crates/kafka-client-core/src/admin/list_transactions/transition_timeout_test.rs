//! Original-deadline and cumulative delivery scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminListTransactionsBrokerOutcome, AdminListTransactionsEffect,
    AdminListTransactionsFailureKind, AdminListTransactionsInput, AdminListTransactionsMachine,
    AdminListTransactionsPlan, AdminListTransactionsTerminal,
};

#[test]
fn deadline_and_delivery_are_cumulative_across_discovery_and_fanout() {
    let mut elapsed = AdminListTransactionsMachine::new(
        OperationId::from_raw(2),
        Deadline::from_tick(5),
        unfiltered_plan(),
    );
    let terminal = effect(
        &mut elapsed,
        AdminListTransactionsInput::Start {
            now: Moment::from_tick(5),
        },
    );
    assert_failure(
        terminal,
        AdminListTransactionsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut partial = machine();
    let _ = effect(
        &mut partial,
        AdminListTransactionsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(&mut partial);
    let _ = effect(
        &mut partial,
        AdminListTransactionsInput::BrokersDiscovered {
            broker_ids: vec![1, 2],
        },
    );
    accept(&mut partial);
    let _ = effect(
        &mut partial,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: AdminListTransactionsBrokerOutcome::Listed {
                broker_id: 1,
                unknown_state_filters: Vec::new(),
                transactions: Vec::new(),
            },
        },
    );
    accept(&mut partial);
    let terminal = effect(
        &mut partial,
        AdminListTransactionsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        },
    );
    assert_failure(
        terminal,
        AdminListTransactionsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn machine() -> AdminListTransactionsMachine {
    AdminListTransactionsMachine::new(
        OperationId::from_raw(17),
        Deadline::from_tick(100),
        unfiltered_plan(),
    )
}

fn unfiltered_plan() -> AdminListTransactionsPlan {
    AdminListTransactionsPlan::new(Vec::new(), Vec::new(), None, None)
        .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn accept(machine: &mut AdminListTransactionsMachine) {
    let transition = machine
        .apply(AdminListTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver accepted: {error}"));
    assert!(transition.into_effect().is_none());
}

fn effect(
    machine: &mut AdminListTransactionsMachine,
    input: AdminListTransactionsInput,
) -> AdminListTransactionsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn assert_failure(
    effect: AdminListTransactionsEffect,
    expected_kind: AdminListTransactionsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let AdminListTransactionsEffect::Complete {
        terminal: AdminListTransactionsTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
