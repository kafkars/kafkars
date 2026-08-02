//! Single-flight, deadline, fencing, and terminal initialization scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    TransactionInitializationBrokerCategory, TransactionInitializationBrokerFailure,
    TransactionInitializationEffect, TransactionInitializationFailureKind,
    TransactionInitializationInput, TransactionInitializationMachine,
    TransactionInitializationMachineError, TransactionInitializationPlan,
    TransactionInitializationState, TransactionInitializationTerminal,
    TransactionInitializationTransition, TransactionalOwnerId,
};

#[test]
fn submit_preserves_owner_plan_and_original_absolute_deadline() {
    let owner = owner(7);
    let mut machine = machine(owner, 20);

    let transition = machine
        .apply(
            owner,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(3),
            },
        )
        .unwrap_or_else(|error| panic!("start should submit: {error}"));

    assert!(matches!(
        transition.into_effect(),
        Some(TransactionInitializationEffect::Submit {
            owner_id,
            operation_id,
            deadline,
            plan,
        }) if owner_id == owner
            && operation_id == OperationId::from_raw(11)
            && deadline == Deadline::from_tick(20)
            && plan.transaction_timeout_ms() == 60_000
    ));
    assert_eq!(
        machine.state(),
        TransactionInitializationState::AwaitingDriver
    );
    assert_eq!(
        machine.apply(
            owner,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(4),
            },
        ),
        Err(TransactionInitializationMachineError::InvalidState)
    );
}

#[test]
fn stale_owner_is_rejected_without_observing_or_mutating_lifecycle() {
    let retained = owner(7);
    let stale = owner(8);
    let mut machine = machine(retained, 20);

    assert_eq!(
        machine.apply(
            stale,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(30),
            },
        ),
        Err(TransactionInitializationMachineError::OwnerMismatch {
            expected: retained,
            supplied: stale,
        })
    );
    assert_eq!(machine.state(), TransactionInitializationState::Ready);
    assert_eq!(machine.owner_id(), retained);
}

#[test]
fn pre_driver_failures_are_definitely_unsent_and_terminal_once() {
    let owner = owner(7);
    for (deadline, input, expected) in [
        (
            3,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(3),
            },
            TransactionInitializationFailureKind::DeadlineElapsed,
        ),
        (
            20,
            TransactionInitializationInput::DriverRejected,
            TransactionInitializationFailureKind::DriverRejected,
        ),
        (
            20,
            TransactionInitializationInput::DeadlineElapsed,
            TransactionInitializationFailureKind::DeadlineElapsed,
        ),
    ] {
        let mut machine = machine(owner, deadline);
        if !matches!(input, TransactionInitializationInput::Start { .. }) {
            start(&mut machine, owner);
        }
        let terminal = machine
            .apply(owner, input)
            .unwrap_or_else(|error| panic!("pre-driver failure should settle: {error}"));
        assert_failure(terminal, expected, DeliveryStatus::NotSent);
        assert_eq!(
            machine.apply(owner, TransactionInitializationInput::DriverRejected),
            Err(TransactionInitializationMachineError::AlreadyCompleted)
        );
    }
}

#[test]
fn submitted_failures_preserve_driver_certainty_and_exact_broker_fencing() {
    let owner = owner(7);
    let broker = TransactionInitializationBrokerFailure::new(
        NonZeroI16::new(-47).unwrap_or_else(|| panic!("broker code")),
        TransactionInitializationBrokerCategory::Fenced,
    );
    let cases = [
        (
            TransactionInitializationInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::NotSent,
            },
            TransactionInitializationFailureKind::DeadlineElapsed,
            DeliveryStatus::NotSent,
        ),
        (
            TransactionInitializationInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            TransactionInitializationFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
        (
            TransactionInitializationInput::BrokerRejected { failure: broker },
            TransactionInitializationFailureKind::Broker(broker),
            DeliveryStatus::PossiblySent,
        ),
    ];

    for (input, kind, delivery) in cases {
        let mut machine = submitted(owner);
        let terminal = machine
            .apply(owner, input)
            .unwrap_or_else(|error| panic!("submitted failure should settle: {error}"));
        assert_failure(terminal, kind, delivery);
    }
}

#[test]
fn refreshed_broker_rejection_reuses_exact_owner_operation_deadline_and_plan() {
    let owner = owner(7);
    let mut machine = submitted(owner);

    let transition = machine
        .apply(
            owner,
            TransactionInitializationInput::RetryableBrokerRejected,
        )
        .unwrap_or_else(|error| panic!("refreshed replacement: {error}"));

    assert!(matches!(
        transition.into_effect(),
        Some(TransactionInitializationEffect::Submit {
            owner_id,
            operation_id,
            deadline,
            plan,
        }) if owner_id == owner
            && operation_id == OperationId::from_raw(11)
            && deadline == Deadline::from_tick(20)
            && plan.transaction_timeout_ms() == 60_000
    ));
    assert_eq!(
        machine.state(),
        TransactionInitializationState::AwaitingDriver
    );
}

#[test]
fn broker_success_requires_nonnegative_identity_and_completes_once() {
    let owner = owner(7);
    let mut initialized = submitted(owner);
    let terminal = initialized
        .apply(
            owner,
            TransactionInitializationInput::BrokerInitialized {
                producer_id: 91,
                producer_epoch: 4,
            },
        )
        .unwrap_or_else(|error| panic!("valid identity should settle: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(TransactionInitializationEffect::Complete {
            owner_id,
            operation_id,
            terminal: TransactionInitializationTerminal::Initialized(identity),
        }) if owner_id == owner
            && operation_id == OperationId::from_raw(11)
            && identity.producer_id() == 91
            && identity.producer_epoch() == 4
    ));
    assert_eq!(
        initialized.apply(owner, TransactionInitializationInput::InvalidResponse),
        Err(TransactionInitializationMachineError::AlreadyCompleted)
    );

    for (producer_id, producer_epoch) in [(-1, 0), (0, -1)] {
        let mut invalid = submitted(owner);
        let terminal = invalid
            .apply(
                owner,
                TransactionInitializationInput::BrokerInitialized {
                    producer_id,
                    producer_epoch,
                },
            )
            .unwrap_or_else(|error| panic!("invalid identity should settle: {error}"));
        assert_failure(
            terminal,
            TransactionInitializationFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

fn owner(value: u64) -> TransactionalOwnerId {
    TransactionalOwnerId::from_raw(value)
}

fn machine(owner: TransactionalOwnerId, deadline: u64) -> TransactionInitializationMachine {
    let plan = TransactionInitializationPlan::new(60_000)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    TransactionInitializationMachine::new(
        owner,
        OperationId::from_raw(11),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn start(machine: &mut TransactionInitializationMachine, owner: TransactionalOwnerId) {
    machine
        .apply(
            owner,
            TransactionInitializationInput::Start {
                now: Moment::from_tick(1),
            },
        )
        .unwrap_or_else(|error| panic!("start should submit: {error}"));
}

fn submitted(owner: TransactionalOwnerId) -> TransactionInitializationMachine {
    let mut machine = machine(owner, 20);
    start(&mut machine, owner);
    machine
        .apply(owner, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver should accept: {error}"));
    machine
}

fn assert_failure(
    transition: TransactionInitializationTransition,
    kind: TransactionInitializationFailureKind,
    delivery: DeliveryStatus,
) {
    assert!(matches!(
        transition.into_effect(),
        Some(TransactionInitializationEffect::Complete {
            terminal: TransactionInitializationTerminal::Failed(failure),
            ..
        }) if failure.kind() == kind && failure.delivery() == delivery
    ));
}
