//! Deadline, sole `AnyBroker` submission, and terminal-assignment scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ExpireDelegationTokenBrokerError, ExpireDelegationTokenEffect, ExpireDelegationTokenFailure,
    ExpireDelegationTokenFailureKind, ExpireDelegationTokenHmac, ExpireDelegationTokenInput,
    ExpireDelegationTokenMachine, ExpireDelegationTokenMachineError, ExpireDelegationTokenPlan,
    ExpireDelegationTokenResponse, ExpireDelegationTokenState, ExpireDelegationTokenTerminal,
};

#[test]
fn sole_submission_transfers_identity_deadline_period_and_unique_hmac() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let ExpireDelegationTokenEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(40));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan.broker_expiry_period_ms(), 86_400_000);
    assert_eq!(plan.hmac().as_bytes(), &[1, 2, 3, 4]);
    assert_eq!(machine.state(), ExpireDelegationTokenState::AwaitingDriver);
    assert_eq!(
        machine.apply(ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(ExpireDelegationTokenMachineError::InvalidState)
    );

    assert!(
        machine
            .apply(ExpireDelegationTokenInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), ExpireDelegationTokenState::Submitted);
}

#[test]
fn pre_handoff_deadline_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(
        elapsed.kind(),
        ExpireDelegationTokenFailureKind::DeadlineElapsed
    );
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        ExpireDelegationTokenInput::DriverRejected,
    ));
    assert_eq!(
        rejected.kind(),
        ExpireDelegationTokenFailureKind::DriverRejected
    );
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn success_and_exact_rejection_assign_one_terminal() {
    let expired = effect(
        &mut submitted_machine(),
        ExpireDelegationTokenInput::BrokerResponded {
            response: ExpireDelegationTokenResponse::new(17, 9_999)
                .unwrap_or_else(|error| panic!("response: {error}")),
        },
    );
    assert_eq!(
        expired,
        ExpireDelegationTokenEffect::Complete {
            operation_id: OperationId::from_raw(40),
            terminal: ExpireDelegationTokenTerminal::Expired(
                super::ExpireDelegationTokenSuccess::new(17, 9_999),
            ),
        }
    );

    let error = ExpireDelegationTokenBrokerError::new(
        23,
        NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            ExpireDelegationTokenInput::BrokerRejected { error },
        ),
        ExpireDelegationTokenEffect::Complete {
            operation_id: OperationId::from_raw(40),
            terminal: ExpireDelegationTokenTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn mechanism_failures_preserve_delivery_without_retry() {
    for (input, kind, delivery) in [
        (
            ExpireDelegationTokenInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            ExpireDelegationTokenFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            ExpireDelegationTokenInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            ExpireDelegationTokenFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            ExpireDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            ExpireDelegationTokenFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }
    for (input, kind) in [
        (
            ExpireDelegationTokenInput::ResponseTooLarge,
            ExpireDelegationTokenFailureKind::ResponseTooLarge,
        ),
        (
            ExpireDelegationTokenInput::InvalidResponse,
            ExpireDelegationTokenFailureKind::InvalidResponse,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(ExpireDelegationTokenInput::DriverAccepted),
        Err(ExpireDelegationTokenMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(ExpireDelegationTokenInput::DriverRejected),
        Err(ExpireDelegationTokenMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(ExpireDelegationTokenInput::InvalidResponse),
        Err(ExpireDelegationTokenMachineError::AlreadyCompleted)
    );
}

fn machine() -> ExpireDelegationTokenMachine {
    ExpireDelegationTokenMachine::new(
        OperationId::from_raw(40),
        Deadline::from_tick(100),
        ExpireDelegationTokenPlan::new(hmac(), Some(86_400_000))
            .unwrap_or_else(|error| panic!("plan: {error}")),
    )
}

fn awaiting_machine() -> ExpireDelegationTokenMachine {
    let mut machine = machine();
    drop(effect(
        &mut machine,
        ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    ));
    machine
}

fn submitted_machine() -> ExpireDelegationTokenMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(ExpireDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn hmac() -> ExpireDelegationTokenHmac {
    ExpireDelegationTokenHmac::new(vec![1, 2, 3, 4]).unwrap_or_else(|error| panic!("hmac: {error}"))
}

fn effect(
    machine: &mut ExpireDelegationTokenMachine,
    input: ExpireDelegationTokenInput,
) -> ExpireDelegationTokenEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: ExpireDelegationTokenEffect) -> ExpireDelegationTokenFailure {
    match effect {
        ExpireDelegationTokenEffect::Complete {
            terminal: ExpireDelegationTokenTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
