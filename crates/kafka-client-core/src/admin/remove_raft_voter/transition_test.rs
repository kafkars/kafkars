//! Deadline, one-attempt handoff, exact error, and terminality scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    RemoveRaftVoterBrokerError, RemoveRaftVoterEffect, RemoveRaftVoterFailure,
    RemoveRaftVoterFailureKind, RemoveRaftVoterInput, RemoveRaftVoterMachine,
    RemoveRaftVoterMachineError, RemoveRaftVoterPlan, RemoveRaftVoterState, RemoveRaftVoterSuccess,
    RemoveRaftVoterTerminal,
};

#[test]
fn sole_submission_reuses_original_identity_deadline_and_plan() {
    let mut machine = machine();
    assert_eq!(
        effect(
            &mut machine,
            RemoveRaftVoterInput::Start {
                now: Moment::from_tick(1),
            },
        ),
        RemoveRaftVoterEffect::Submit {
            operation_id: OperationId::from_raw(81),
            deadline: Deadline::from_tick(100),
            plan: plan(),
        }
    );
    assert_eq!(machine.state(), RemoveRaftVoterState::AwaitingDriver);
    assert!(
        machine
            .apply(RemoveRaftVoterInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), RemoveRaftVoterState::Submitted);
}

#[test]
fn pre_handoff_expiry_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        RemoveRaftVoterInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(elapsed.kind(), RemoveRaftVoterFailureKind::DeadlineElapsed);
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        RemoveRaftVoterInput::DriverRejected,
    ));
    assert_eq!(rejected.kind(), RemoveRaftVoterFailureKind::DriverRejected);
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn post_handoff_failures_preserve_certainty_without_emitting_retry() {
    for (input, kind, delivery) in [
        (
            RemoveRaftVoterInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            RemoveRaftVoterFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RemoveRaftVoterInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            RemoveRaftVoterFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            RemoveRaftVoterInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            RemoveRaftVoterFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }

    for (input, kind) in [
        (
            RemoveRaftVoterInput::ResponseTooLarge,
            RemoveRaftVoterFailureKind::ResponseTooLarge,
        ),
        (
            RemoveRaftVoterInput::InvalidResponse,
            RemoveRaftVoterFailureKind::InvalidResponse,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn success_and_exact_signed_broker_rejection_are_distinct_terminals() {
    let success = RemoveRaftVoterSuccess::new(7);
    assert_eq!(
        effect(
            &mut submitted_machine(),
            RemoveRaftVoterInput::BrokerResponded { success },
        ),
        RemoveRaftVoterEffect::Complete {
            operation_id: OperationId::from_raw(81),
            terminal: RemoveRaftVoterTerminal::Removed(success),
        }
    );

    let error = RemoveRaftVoterBrokerError::new(
        11,
        NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
        Some("future error".to_owned()),
        false,
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            RemoveRaftVoterInput::BrokerRejected {
                error: error.clone(),
            },
        ),
        RemoveRaftVoterEffect::Complete {
            operation_id: OperationId::from_raw(81),
            terminal: RemoveRaftVoterTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn hostile_broker_diagnostic_becomes_invalid_response() {
    let error = RemoveRaftVoterBrokerError::new(
        0,
        NonZeroI16::new(1).unwrap_or_else(|| panic!("nonzero")),
        None,
        true,
    );
    let failure = failure(effect(
        &mut submitted_machine(),
        RemoveRaftVoterInput::BrokerRejected { error },
    ));
    assert_eq!(failure.kind(), RemoveRaftVoterFailureKind::InvalidResponse);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(RemoveRaftVoterInput::DriverAccepted),
        Err(RemoveRaftVoterMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(RemoveRaftVoterInput::DriverRejected),
        Err(RemoveRaftVoterMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        RemoveRaftVoterInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(RemoveRaftVoterInput::InvalidResponse),
        Err(RemoveRaftVoterMachineError::AlreadyCompleted)
    );
}

fn plan() -> RemoveRaftVoterPlan {
    RemoveRaftVoterPlan::new(Some("cluster-a".to_owned()), 7, [9; 16])
        .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine() -> RemoveRaftVoterMachine {
    RemoveRaftVoterMachine::new(OperationId::from_raw(81), Deadline::from_tick(100), plan())
}

fn awaiting_machine() -> RemoveRaftVoterMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        RemoveRaftVoterInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> RemoveRaftVoterMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(RemoveRaftVoterInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn effect(
    machine: &mut RemoveRaftVoterMachine,
    input: RemoveRaftVoterInput,
) -> RemoveRaftVoterEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: RemoveRaftVoterEffect) -> RemoveRaftVoterFailure {
    match effect {
        RemoveRaftVoterEffect::Complete {
            terminal: RemoveRaftVoterTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
