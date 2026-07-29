//! Deadline, one-attempt handoff, exact error, and terminality scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AddRaftVoterBrokerError, AddRaftVoterEffect, AddRaftVoterEndpoint, AddRaftVoterFailure,
    AddRaftVoterFailureKind, AddRaftVoterInput, AddRaftVoterMachine, AddRaftVoterMachineError,
    AddRaftVoterPlan, AddRaftVoterState, AddRaftVoterSuccess, AddRaftVoterTerminal,
};

#[test]
fn sole_submission_reuses_original_identity_deadline_and_plan() {
    let mut machine = machine();
    assert_eq!(
        effect(
            &mut machine,
            AddRaftVoterInput::Start {
                now: Moment::from_tick(1),
            },
        ),
        AddRaftVoterEffect::Submit {
            operation_id: OperationId::from_raw(80),
            deadline: Deadline::from_tick(100),
            plan: plan(),
        }
    );
    assert_eq!(machine.state(), AddRaftVoterState::AwaitingDriver);
    assert!(
        machine
            .apply(AddRaftVoterInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), AddRaftVoterState::Submitted);
}

#[test]
fn pre_handoff_expiry_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        AddRaftVoterInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(elapsed.kind(), AddRaftVoterFailureKind::DeadlineElapsed);
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        AddRaftVoterInput::DriverRejected,
    ));
    assert_eq!(rejected.kind(), AddRaftVoterFailureKind::DriverRejected);
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn post_handoff_failures_preserve_certainty_without_emitting_retry() {
    for (input, kind, delivery) in [
        (
            AddRaftVoterInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AddRaftVoterFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AddRaftVoterInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            AddRaftVoterFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            AddRaftVoterInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            AddRaftVoterFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }

    for (input, kind) in [
        (
            AddRaftVoterInput::ResponseTooLarge,
            AddRaftVoterFailureKind::ResponseTooLarge,
        ),
        (
            AddRaftVoterInput::InvalidResponse,
            AddRaftVoterFailureKind::InvalidResponse,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn success_and_exact_signed_broker_rejection_are_distinct_terminals() {
    let success = AddRaftVoterSuccess::new(7);
    assert_eq!(
        effect(
            &mut submitted_machine(),
            AddRaftVoterInput::BrokerResponded { success },
        ),
        AddRaftVoterEffect::Complete {
            operation_id: OperationId::from_raw(80),
            terminal: AddRaftVoterTerminal::Added(success),
        }
    );

    let error = AddRaftVoterBrokerError::new(
        11,
        NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
        Some("future error".to_owned()),
        false,
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            AddRaftVoterInput::BrokerRejected {
                error: error.clone(),
            },
        ),
        AddRaftVoterEffect::Complete {
            operation_id: OperationId::from_raw(80),
            terminal: AddRaftVoterTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn hostile_broker_diagnostic_becomes_invalid_response() {
    let error = AddRaftVoterBrokerError::new(
        0,
        NonZeroI16::new(1).unwrap_or_else(|| panic!("nonzero")),
        None,
        true,
    );
    let failure = failure(effect(
        &mut submitted_machine(),
        AddRaftVoterInput::BrokerRejected { error },
    ));
    assert_eq!(failure.kind(), AddRaftVoterFailureKind::InvalidResponse);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(AddRaftVoterInput::DriverAccepted),
        Err(AddRaftVoterMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(AddRaftVoterInput::DriverRejected),
        Err(AddRaftVoterMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        AddRaftVoterInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(AddRaftVoterInput::InvalidResponse),
        Err(AddRaftVoterMachineError::AlreadyCompleted)
    );
}

fn plan() -> AddRaftVoterPlan {
    AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![AddRaftVoterEndpoint::new(
            "CONTROLLER".to_owned(),
            "node-a".to_owned(),
            9093,
        )],
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine() -> AddRaftVoterMachine {
    AddRaftVoterMachine::new(OperationId::from_raw(80), Deadline::from_tick(100), plan())
}

fn awaiting_machine() -> AddRaftVoterMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        AddRaftVoterInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> AddRaftVoterMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(AddRaftVoterInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn effect(machine: &mut AddRaftVoterMachine, input: AddRaftVoterInput) -> AddRaftVoterEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: AddRaftVoterEffect) -> AddRaftVoterFailure {
    match effect {
        AddRaftVoterEffect::Complete {
            terminal: AddRaftVoterTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
