//! Deadline, destructive handoff, exact rejection, and terminality scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    UNREGISTER_BROKER_DIAGNOSTIC_BYTES, UnregisterBrokerBrokerError, UnregisterBrokerEffect,
    UnregisterBrokerFailure, UnregisterBrokerFailureKind, UnregisterBrokerInput,
    UnregisterBrokerMachine, UnregisterBrokerMachineError, UnregisterBrokerPlan,
    UnregisterBrokerState, UnregisterBrokerSuccess, UnregisterBrokerTerminal,
};

#[test]
fn sole_submission_reuses_original_identity_deadline_and_broker() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        UnregisterBrokerInput::Start {
            now: Moment::from_tick(1),
        },
    );

    assert_eq!(
        submit,
        UnregisterBrokerEffect::Submit {
            operation_id: OperationId::from_raw(64),
            deadline: Deadline::from_tick(100),
            plan: plan(),
        }
    );
    assert_eq!(machine.state(), UnregisterBrokerState::AwaitingDriver);
    assert!(
        machine
            .apply(UnregisterBrokerInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), UnregisterBrokerState::Submitted);
}

#[test]
fn pre_handoff_expiry_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        UnregisterBrokerInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(elapsed.kind(), UnregisterBrokerFailureKind::DeadlineElapsed);
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        UnregisterBrokerInput::DriverRejected,
    ));
    assert_eq!(rejected.kind(), UnregisterBrokerFailureKind::DriverRejected);
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn post_handoff_mechanism_failures_preserve_delivery_certainty_without_retry() {
    for (input, kind, delivery) in [
        (
            UnregisterBrokerInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            UnregisterBrokerFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            UnregisterBrokerInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            UnregisterBrokerFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            UnregisterBrokerInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            UnregisterBrokerFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }

    for (input, kind) in [
        (
            UnregisterBrokerInput::ResponseTooLarge,
            UnregisterBrokerFailureKind::ResponseTooLarge,
        ),
        (
            UnregisterBrokerInput::InvalidResponse,
            UnregisterBrokerFailureKind::InvalidResponse,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn success_and_exact_broker_rejection_publish_distinct_terminals() {
    let success = UnregisterBrokerSuccess::new(17);
    assert_eq!(
        effect(
            &mut submitted_machine(),
            UnregisterBrokerInput::BrokerResponded { success },
        ),
        UnregisterBrokerEffect::Complete {
            operation_id: OperationId::from_raw(64),
            terminal: UnregisterBrokerTerminal::Unregistered(success),
        }
    );

    let error = broker_error(Some("not controller".to_owned()), false);
    assert!(matches!(
        effect(
            &mut submitted_machine(),
            UnregisterBrokerInput::BrokerRejected {
                error: error.clone()
            },
        ),
        UnregisterBrokerEffect::Complete {
            terminal: UnregisterBrokerTerminal::BrokerRejected(actual),
            ..
        } if actual == error
    ));
}

#[test]
fn unbounded_or_contradictory_diagnostic_becomes_invalid_response() {
    let oversized = broker_error(
        Some("x".repeat(UNREGISTER_BROKER_DIAGNOSTIC_BYTES + 1)),
        true,
    );
    for error in [oversized, broker_error(None, true)] {
        let failure = failure(effect(
            &mut submitted_machine(),
            UnregisterBrokerInput::BrokerRejected { error },
        ));
        assert_eq!(failure.kind(), UnregisterBrokerFailureKind::InvalidResponse);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(UnregisterBrokerInput::DriverAccepted),
        Err(UnregisterBrokerMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(UnregisterBrokerInput::DriverRejected),
        Err(UnregisterBrokerMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        UnregisterBrokerInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(UnregisterBrokerInput::InvalidResponse),
        Err(UnregisterBrokerMachineError::AlreadyCompleted)
    );
}

fn plan() -> UnregisterBrokerPlan {
    UnregisterBrokerPlan::new(7).unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine() -> UnregisterBrokerMachine {
    UnregisterBrokerMachine::new(OperationId::from_raw(64), Deadline::from_tick(100), plan())
}

fn awaiting_machine() -> UnregisterBrokerMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        UnregisterBrokerInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> UnregisterBrokerMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(UnregisterBrokerInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn broker_error(message: Option<String>, truncated: bool) -> UnregisterBrokerBrokerError {
    UnregisterBrokerBrokerError::new(
        23,
        NonZeroI16::new(-41).unwrap_or_else(|| panic!("nonzero")),
        message,
        truncated,
    )
}

fn effect(
    machine: &mut UnregisterBrokerMachine,
    input: UnregisterBrokerInput,
) -> UnregisterBrokerEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: UnregisterBrokerEffect) -> UnregisterBrokerFailure {
    match effect {
        UnregisterBrokerEffect::Complete {
            terminal: UnregisterBrokerTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
