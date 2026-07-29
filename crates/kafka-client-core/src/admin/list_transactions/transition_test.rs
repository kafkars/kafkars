//! Discovery, all-broker fanout, aggregation, bounds, and terminal scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminListTransactionsBrokerError, AdminListTransactionsBrokerOutcome,
    AdminListTransactionsEffect, AdminListTransactionsFailureKind, AdminListTransactionsInput,
    AdminListTransactionsMachine, AdminListTransactionsPlan, AdminListTransactionsTerminal,
    AdminListedTransaction, LIST_TRANSACTIONS_MAX_BROKERS,
    LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES,
};

#[test]
fn discovery_and_each_broker_reuse_the_original_public_deadline() {
    let mut machine = machine(filtered_plan());
    let discovery = effect(
        &mut machine,
        AdminListTransactionsInput::Start {
            now: Moment::from_tick(2),
        },
    );
    assert!(matches!(
        discovery,
        AdminListTransactionsEffect::SubmitDiscovery {
            operation_id,
            deadline,
        } if operation_id == OperationId::from_raw(17)
            && deadline == Deadline::from_tick(100)
    ));
    accept(&mut machine);
    let first = effect(
        &mut machine,
        AdminListTransactionsInput::BrokersDiscovered {
            broker_ids: vec![9, 2],
        },
    );
    let AdminListTransactionsEffect::SubmitBroker {
        broker_id,
        deadline,
        plan,
        ..
    } = first
    else {
        panic!("expected broker submission");
    };
    assert_eq!(broker_id, 2);
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan, filtered_plan());
}

#[test]
fn all_brokers_settle_with_deterministic_deduplication_and_exact_errors() {
    let mut machine = machine(filtered_plan());
    discover(&mut machine, vec![8, 3, 5]);

    accept(&mut machine);
    let next = effect(
        &mut machine,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 7,
            outcome: listed(
                3,
                &["UnknownB", "UnknownA"],
                &[
                    transaction("zeta", -7, "Ongoing"),
                    transaction("same", 4, "PrepareCommit"),
                ],
            ),
        },
    );
    assert!(matches!(
        next,
        AdminListTransactionsEffect::SubmitBroker { broker_id: 5, .. }
    ));

    accept(&mut machine);
    let next = effect(
        &mut machine,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: AdminListTransactionsBrokerOutcome::Rejected(
                AdminListTransactionsBrokerError::new(
                    5,
                    NonZeroI16::new(-45).unwrap_or_else(|| panic!("nonzero")),
                ),
            ),
        },
    );
    assert!(matches!(
        next,
        AdminListTransactionsEffect::SubmitBroker { broker_id: 8, .. }
    ));

    accept(&mut machine);
    let terminal = effect(
        &mut machine,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcome: listed(
                8,
                &["UnknownA"],
                &[
                    transaction("alpha", i64::MIN, "Empty"),
                    transaction("same", 4, "PrepareCommit"),
                ],
            ),
        },
    );
    let AdminListTransactionsEffect::Complete {
        terminal: AdminListTransactionsTerminal::Listed(batch),
        ..
    } = terminal
    else {
        panic!("expected listed terminal");
    };
    assert_eq!(batch.throttle_time_ms(), 11);
    assert_eq!(batch.unknown_state_filters(), ["UnknownA", "UnknownB"]);
    assert_eq!(
        batch
            .transactions()
            .iter()
            .map(|transaction| transaction.transactional_id())
            .collect::<Vec<_>>(),
        ["alpha", "same", "zeta"]
    );
    assert_eq!(batch.transactions()[0].producer_id(), i64::MIN);
    assert_eq!(batch.broker_errors()[0].into_parts(), (5, -45));
}

#[test]
fn contradictory_duplicate_transaction_facts_are_invalid_response() {
    let mut machine = machine(unfiltered_plan());
    discover(&mut machine, vec![1, 2]);
    accept(&mut machine);
    let _ = effect(
        &mut machine,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: listed(1, &[], &[transaction("same", 4, "Ongoing")]),
        },
    );
    accept(&mut machine);
    let terminal = effect(
        &mut machine,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: listed(2, &[], &[transaction("same", 5, "Ongoing")]),
        },
    );
    assert_failure(
        terminal,
        AdminListTransactionsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn discovered_and_result_capacity_bounds_fail_explicitly() {
    let mut too_many_brokers = machine(unfiltered_plan());
    start_and_accept(&mut too_many_brokers);
    let terminal = effect(
        &mut too_many_brokers,
        AdminListTransactionsInput::BrokersDiscovered {
            broker_ids: (0..=LIST_TRANSACTIONS_MAX_BROKERS)
                .map(|broker| broker as i32)
                .collect(),
        },
    );
    assert_failure(
        terminal,
        AdminListTransactionsFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );

    let mut oversized_result = machine(unfiltered_plan());
    discover(&mut oversized_result, vec![3]);
    accept(&mut oversized_result);
    let terminal = effect(
        &mut oversized_result,
        AdminListTransactionsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: listed(
                3,
                &[],
                &[transaction(
                    "id",
                    1,
                    &"x".repeat(LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES + 1),
                )],
            ),
        },
    );
    assert_failure(
        terminal,
        AdminListTransactionsFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );
}

fn machine(plan: AdminListTransactionsPlan) -> AdminListTransactionsMachine {
    AdminListTransactionsMachine::new(OperationId::from_raw(17), Deadline::from_tick(100), plan)
}

fn filtered_plan() -> AdminListTransactionsPlan {
    AdminListTransactionsPlan::new(
        vec!["Ongoing".to_owned()],
        vec![-7],
        Some(250),
        Some("^orders".to_owned()),
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn unfiltered_plan() -> AdminListTransactionsPlan {
    AdminListTransactionsPlan::new(Vec::new(), Vec::new(), None, None)
        .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn start_and_accept(machine: &mut AdminListTransactionsMachine) {
    let _ = effect(
        machine,
        AdminListTransactionsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(machine);
}

fn discover(machine: &mut AdminListTransactionsMachine, broker_ids: Vec<i32>) {
    start_and_accept(machine);
    let _ = effect(
        machine,
        AdminListTransactionsInput::BrokersDiscovered { broker_ids },
    );
}

fn accept(machine: &mut AdminListTransactionsMachine) {
    let transition = machine
        .apply(AdminListTransactionsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver accepted: {error}"));
    assert!(transition.into_effect().is_none());
}

fn listed(
    broker_id: i32,
    unknown_state_filters: &[&str],
    transactions: &[AdminListedTransaction],
) -> AdminListTransactionsBrokerOutcome {
    AdminListTransactionsBrokerOutcome::Listed {
        broker_id,
        unknown_state_filters: unknown_state_filters
            .iter()
            .map(|state| (*state).to_owned())
            .collect(),
        transactions: transactions.to_vec(),
    }
}

fn transaction(id: &str, producer_id: i64, state: &str) -> AdminListedTransaction {
    AdminListedTransaction::new(id.to_owned(), producer_id, state.to_owned())
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
