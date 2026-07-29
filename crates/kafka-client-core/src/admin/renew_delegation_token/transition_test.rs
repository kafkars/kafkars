//! Deadline, sole AnyBroker submission, and terminal-assignment scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    RenewDelegationTokenBrokerError, RenewDelegationTokenEffect, RenewDelegationTokenFailure,
    RenewDelegationTokenFailureKind, RenewDelegationTokenHmac, RenewDelegationTokenInput,
    RenewDelegationTokenMachine, RenewDelegationTokenMachineError, RenewDelegationTokenPlan,
    RenewDelegationTokenResponse, RenewDelegationTokenState, RenewDelegationTokenTerminal,
};

#[test]
fn sole_submission_transfers_identity_deadline_period_and_unique_hmac() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        RenewDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let RenewDelegationTokenEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(39));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan.broker_renew_period_ms(), 86_400_000);
    assert_eq!(plan.hmac().as_bytes(), &[1, 2, 3, 4]);
    assert_eq!(machine.state(), RenewDelegationTokenState::AwaitingDriver);
    assert_eq!(
        machine.apply(RenewDelegationTokenInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(RenewDelegationTokenMachineError::InvalidState)
    );

    assert!(
        machine
            .apply(RenewDelegationTokenInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), RenewDelegationTokenState::Submitted);
}

#[test]
fn pre_handoff_deadline_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        RenewDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(
        elapsed.kind(),
        RenewDelegationTokenFailureKind::DeadlineElapsed
    );
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        RenewDelegationTokenInput::DriverRejected,
    ));
    assert_eq!(
        rejected.kind(),
        RenewDelegationTokenFailureKind::DriverRejected
    );
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn success_and_exact_rejection_assign_one_terminal() {
    let renewed = effect(
        &mut submitted_machine(),
        RenewDelegationTokenInput::BrokerResponded {
            response: RenewDelegationTokenResponse::new(17, 9_999)
                .unwrap_or_else(|error| panic!("response: {error}")),
        },
    );
    assert_eq!(
        renewed,
        RenewDelegationTokenEffect::Complete {
            operation_id: OperationId::from_raw(39),
            terminal: RenewDelegationTokenTerminal::Renewed(
                super::RenewDelegationTokenSuccess::new(17, 9_999),
            ),
        }
    );

    let error = RenewDelegationTokenBrokerError::new(
        23,
        NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            RenewDelegationTokenInput::BrokerRejected { error },
        ),
        RenewDelegationTokenEffect::Complete {
            operation_id: OperationId::from_raw(39),
            terminal: RenewDelegationTokenTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn mechanism_failures_preserve_delivery_without_retry() {
    for (input, kind, delivery) in [
        (
            RenewDelegationTokenInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            RenewDelegationTokenFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RenewDelegationTokenInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            RenewDelegationTokenFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            RenewDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            RenewDelegationTokenFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }
    for (input, kind) in [
        (
            RenewDelegationTokenInput::ResponseTooLarge,
            RenewDelegationTokenFailureKind::ResponseTooLarge,
        ),
        (
            RenewDelegationTokenInput::InvalidResponse,
            RenewDelegationTokenFailureKind::InvalidResponse,
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
        machine().apply(RenewDelegationTokenInput::DriverAccepted),
        Err(RenewDelegationTokenMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(RenewDelegationTokenInput::DriverRejected),
        Err(RenewDelegationTokenMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        RenewDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(RenewDelegationTokenInput::InvalidResponse),
        Err(RenewDelegationTokenMachineError::AlreadyCompleted)
    );
}

fn machine() -> RenewDelegationTokenMachine {
    RenewDelegationTokenMachine::new(
        OperationId::from_raw(39),
        Deadline::from_tick(100),
        RenewDelegationTokenPlan::new(hmac(), Some(86_400_000))
            .unwrap_or_else(|error| panic!("plan: {error}")),
    )
}

fn awaiting_machine() -> RenewDelegationTokenMachine {
    let mut machine = machine();
    drop(effect(
        &mut machine,
        RenewDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    ));
    machine
}

fn submitted_machine() -> RenewDelegationTokenMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(RenewDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn hmac() -> RenewDelegationTokenHmac {
    RenewDelegationTokenHmac::new(vec![1, 2, 3, 4]).unwrap_or_else(|error| panic!("hmac: {error}"))
}

fn effect(
    machine: &mut RenewDelegationTokenMachine,
    input: RenewDelegationTokenInput,
) -> RenewDelegationTokenEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: RenewDelegationTokenEffect) -> RenewDelegationTokenFailure {
    match effect {
        RenewDelegationTokenEffect::Complete {
            terminal: RenewDelegationTokenTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
