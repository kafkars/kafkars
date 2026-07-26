//! Core-to-engine transaction initialization terminal translation.

use std::{num::NonZeroI16, sync::Arc};

use kafka_client_core::{
    Deadline, Moment, OperationId, TransactionInitializationBrokerCategory,
    TransactionInitializationBrokerFailure, TransactionInitializationEffect,
    TransactionInitializationInput, TransactionInitializationMachine,
    TransactionInitializationPlan, TransactionalOwnerId,
};

use super::{
    TransactionInitializationDeliveryStatus, TransactionInitializationFailureKind,
    TransactionInitializationOutcome, outcome::failed_retained_outcome,
};

#[test]
fn core_terminal_translation_retains_exact_broker_code_fencing_and_delivery() {
    let owner = TransactionalOwnerId::from_raw(7);
    let plan = TransactionInitializationPlan::new(60_000)
        .unwrap_or_else(|error| panic!("valid transaction plan: {error}"));
    let mut machine = TransactionInitializationMachine::new(
        owner,
        OperationId::from_raw(11),
        Deadline::from_tick(20),
        plan,
    );
    machine
        .apply(
            owner,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(1),
            },
        )
        .unwrap_or_else(|error| panic!("start initialization: {error}"));
    machine
        .apply(owner, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver accepts initialization: {error}"));
    let broker = TransactionInitializationBrokerFailure::new(
        NonZeroI16::new(-47).unwrap_or_else(|| panic!("nonzero broker code")),
        TransactionInitializationBrokerCategory::Fenced,
    );
    let transition = machine
        .apply(
            owner,
            TransactionInitializationInput::BrokerRejected { failure: broker },
        )
        .unwrap_or_else(|error| panic!("broker rejection settles initialization: {error}"));
    let terminal = match transition.into_effect() {
        Some(TransactionInitializationEffect::Complete { terminal, .. }) => terminal,
        effect => panic!("expected terminal completion, got {effect:?}"),
    };
    let retained = failed_retained_outcome(terminal)
        .unwrap_or_else(|| panic!("failed core terminal must translate"));
    let TransactionInitializationOutcome::Failed(failure) = retained.into_observed(Arc::new(()))
    else {
        panic!("failed core terminal became initialized");
    };

    assert_eq!(
        failure.kind(),
        TransactionInitializationFailureKind::Broker {
            code: -47,
            fenced: true,
        }
    );
    assert_eq!(
        failure.delivery(),
        TransactionInitializationDeliveryStatus::PossiblySent
    );
}
