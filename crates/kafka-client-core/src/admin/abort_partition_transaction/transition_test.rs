//! Deadline, one-attempt handoff, exact error, and terminality scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AbortPartitionTransactionBrokerError, AbortPartitionTransactionEffect,
    AbortPartitionTransactionFailure, AbortPartitionTransactionFailureKind,
    AbortPartitionTransactionInput, AbortPartitionTransactionMachine,
    AbortPartitionTransactionMachineError, AbortPartitionTransactionPlan,
    AbortPartitionTransactionState, AbortPartitionTransactionTerminal,
};

#[test]
fn sole_submission_reuses_original_identity_deadline_and_plan() {
    let mut machine = machine();
    assert_eq!(
        effect(
            &mut machine,
            AbortPartitionTransactionInput::Start {
                now: Moment::from_tick(1),
            },
        ),
        AbortPartitionTransactionEffect::Submit {
            operation_id: OperationId::from_raw(27),
            deadline: Deadline::from_tick(100),
            plan: plan(),
        }
    );
    assert_eq!(
        machine.state(),
        AbortPartitionTransactionState::AwaitingDriver
    );
    assert!(
        machine
            .apply(AbortPartitionTransactionInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), AbortPartitionTransactionState::Submitted);
}

#[test]
fn pre_handoff_expiry_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        AbortPartitionTransactionInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(
        elapsed.kind(),
        AbortPartitionTransactionFailureKind::DeadlineElapsed
    );
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        AbortPartitionTransactionInput::DriverRejected,
    ));
    assert_eq!(
        rejected.kind(),
        AbortPartitionTransactionFailureKind::DriverRejected
    );
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn post_handoff_failures_preserve_certainty_without_emitting_retry() {
    for (input, kind, delivery) in [
        (
            AbortPartitionTransactionInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AbortPartitionTransactionFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AbortPartitionTransactionInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            AbortPartitionTransactionFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            AbortPartitionTransactionInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            AbortPartitionTransactionFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let terminal = effect(&mut submitted_machine(), input);
        let failure = failure(terminal);
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }

    for (input, kind) in [
        (
            AbortPartitionTransactionInput::ResponseTooLarge,
            AbortPartitionTransactionFailureKind::ResponseTooLarge,
        ),
        (
            AbortPartitionTransactionInput::InvalidResponse,
            AbortPartitionTransactionFailureKind::InvalidResponse,
        ),
    ] {
        let terminal = effect(&mut submitted_machine(), input);
        let failure = failure(terminal);
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn success_and_exact_signed_broker_rejection_are_distinct_terminals() {
    assert_eq!(
        effect(
            &mut submitted_machine(),
            AbortPartitionTransactionInput::BrokerResponded,
        ),
        AbortPartitionTransactionEffect::Complete {
            operation_id: OperationId::from_raw(27),
            terminal: AbortPartitionTransactionTerminal::Aborted,
        }
    );

    let error = AbortPartitionTransactionBrokerError::new(
        NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            AbortPartitionTransactionInput::BrokerRejected { error },
        ),
        AbortPartitionTransactionEffect::Complete {
            operation_id: OperationId::from_raw(27),
            terminal: AbortPartitionTransactionTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(AbortPartitionTransactionInput::DriverAccepted),
        Err(AbortPartitionTransactionMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(AbortPartitionTransactionInput::DriverRejected),
        Err(AbortPartitionTransactionMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        AbortPartitionTransactionInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(AbortPartitionTransactionInput::InvalidResponse),
        Err(AbortPartitionTransactionMachineError::AlreadyCompleted)
    );
}

fn plan() -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new("orders".to_owned(), 2, 91, 7, 11)
        .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine() -> AbortPartitionTransactionMachine {
    AbortPartitionTransactionMachine::new(
        OperationId::from_raw(27),
        Deadline::from_tick(100),
        plan(),
    )
}

fn awaiting_machine() -> AbortPartitionTransactionMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        AbortPartitionTransactionInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> AbortPartitionTransactionMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(AbortPartitionTransactionInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn effect(
    machine: &mut AbortPartitionTransactionMachine,
    input: AbortPartitionTransactionInput,
) -> AbortPartitionTransactionEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: AbortPartitionTransactionEffect) -> AbortPartitionTransactionFailure {
    match effect {
        AbortPartitionTransactionEffect::Complete {
            terminal: AbortPartitionTransactionTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
