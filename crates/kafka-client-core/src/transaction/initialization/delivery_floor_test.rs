//! Monotonic delivery certainty across transaction-initialization replacements.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    TransactionInitializationEffect, TransactionInitializationFailureKind,
    TransactionInitializationInput, TransactionInitializationMachine,
    TransactionInitializationPlan, TransactionInitializationTerminal, TransactionalOwnerId,
};

#[test]
fn uncertain_retry_evidence_survives_every_replacement_failure_stage() {
    let cases = [
        (
            false,
            TransactionInitializationInput::DriverRejected,
            TransactionInitializationFailureKind::DriverRejected,
        ),
        (
            false,
            TransactionInitializationInput::DeadlineElapsed,
            TransactionInitializationFailureKind::DeadlineElapsed,
        ),
        (
            true,
            TransactionInitializationInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            TransactionInitializationFailureKind::Transport,
        ),
    ];

    for (accept_replacement, terminal, expected_kind) in cases {
        let (owner, mut machine) = retrying(DeliveryStatus::PossiblySent);
        if accept_replacement {
            machine
                .apply(owner, TransactionInitializationInput::DriverAccepted)
                .unwrap_or_else(|error| panic!("replacement acceptance: {error}"));
        }
        let transition = machine
            .apply(owner, terminal)
            .unwrap_or_else(|error| panic!("replacement terminal: {error}"));
        assert_failure(
            transition.into_effect(),
            expected_kind,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn definitely_unsent_retry_evidence_does_not_invent_uncertainty() {
    let (owner, mut machine) = retrying(DeliveryStatus::NotSent);
    let transition = machine
        .apply(owner, TransactionInitializationInput::DriverRejected)
        .unwrap_or_else(|error| panic!("replacement rejection: {error}"));

    assert_failure(
        transition.into_effect(),
        TransactionInitializationFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn successful_replacement_may_initialize_after_uncertain_attempt() {
    let (owner, mut machine) = retrying(DeliveryStatus::PossiblySent);
    machine
        .apply(owner, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("replacement acceptance: {error}"));
    let transition = machine
        .apply(
            owner,
            TransactionInitializationInput::BrokerInitialized {
                producer_id: 41,
                producer_epoch: 3,
            },
        )
        .unwrap_or_else(|error| panic!("replacement initialization: {error}"));

    assert!(matches!(
        transition.into_effect(),
        Some(TransactionInitializationEffect::Complete {
            terminal: TransactionInitializationTerminal::Initialized(identity),
            ..
        }) if identity.producer_id() == 41 && identity.producer_epoch() == 3
    ));
}

#[test]
fn repeated_uncertain_retry_authority_never_strengthens_delivery() {
    let (owner, mut machine) = retrying(DeliveryStatus::PossiblySent);
    machine
        .apply(owner, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("first replacement acceptance: {error}"));
    machine
        .apply(
            owner,
            TransactionInitializationInput::RetryAuthorized {
                delivery: DeliveryStatus::PossiblySent,
            },
        )
        .unwrap_or_else(|error| panic!("second retry authority: {error}"));
    let transition = machine
        .apply(owner, TransactionInitializationInput::DriverRejected)
        .unwrap_or_else(|error| panic!("second replacement rejection: {error}"));

    assert_failure(
        transition.into_effect(),
        TransactionInitializationFailureKind::DriverRejected,
        DeliveryStatus::PossiblySent,
    );
}

fn retrying(delivery: DeliveryStatus) -> (TransactionalOwnerId, TransactionInitializationMachine) {
    let owner = TransactionalOwnerId::from_raw(7);
    let plan = TransactionInitializationPlan::new(60_000)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
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
        .unwrap_or_else(|error| panic!("initial submission: {error}"));
    machine
        .apply(owner, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("initial acceptance: {error}"));
    machine
        .apply(
            owner,
            TransactionInitializationInput::RetryAuthorized { delivery },
        )
        .unwrap_or_else(|error| panic!("retry authority: {error}"));
    (owner, machine)
}

fn assert_failure(
    effect: Option<TransactionInitializationEffect>,
    expected_kind: TransactionInitializationFailureKind,
    expected_delivery: DeliveryStatus,
) {
    assert!(matches!(
        effect,
        Some(TransactionInitializationEffect::Complete {
            terminal: TransactionInitializationTerminal::Failed(failure),
            ..
        }) if failure.kind() == expected_kind && failure.delivery() == expected_delivery
    ));
}
