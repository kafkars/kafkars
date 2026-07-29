//! Deadline, AnyBroker handoff, correlation, and single terminal scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CreateDelegationTokenBrokerError, CreateDelegationTokenEffect, CreateDelegationTokenFailure,
    CreateDelegationTokenFailureKind, CreateDelegationTokenInput, CreateDelegationTokenMachine,
    CreateDelegationTokenMachineError, CreateDelegationTokenPlan, CreateDelegationTokenResponse,
    CreateDelegationTokenState, CreateDelegationTokenTerminal, DelegationTokenHmac,
    DelegationTokenPrincipal,
};

#[test]
fn sole_submission_reuses_identity_deadline_plan_and_v3_owner_requirement() {
    let mut machine = machine();
    let submit = effect(
        &mut machine,
        CreateDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    );

    let CreateDelegationTokenEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = submit
    else {
        panic!("submit expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(38));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(plan.minimum_version(), 3);
    assert_eq!(plan.max_lifetime_ms(), Some(86_400_000));
    assert_eq!(
        plan.renewers()
            .iter()
            .map(DelegationTokenPrincipal::principal_name)
            .collect::<Vec<_>>(),
        vec!["bob", "billing"]
    );
    assert_eq!(machine.state(), CreateDelegationTokenState::AwaitingDriver);

    assert!(
        machine
            .apply(CreateDelegationTokenInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
    assert_eq!(machine.state(), CreateDelegationTokenState::Submitted);
}

#[test]
fn pre_handoff_expiry_and_rejection_are_definitely_unsent() {
    let elapsed = failure(effect(
        &mut machine(),
        CreateDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    ));
    assert_eq!(
        elapsed.kind(),
        CreateDelegationTokenFailureKind::DeadlineElapsed
    );
    assert_eq!(elapsed.delivery(), DeliveryStatus::NotSent);

    let rejected = failure(effect(
        &mut awaiting_machine(),
        CreateDelegationTokenInput::DriverRejected,
    ));
    assert_eq!(
        rejected.kind(),
        CreateDelegationTokenFailureKind::DriverRejected
    );
    assert_eq!(rejected.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn post_handoff_failures_preserve_delivery_without_retry() {
    for (input, kind, delivery) in [
        (
            CreateDelegationTokenInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            CreateDelegationTokenFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            CreateDelegationTokenInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            CreateDelegationTokenFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            CreateDelegationTokenInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            CreateDelegationTokenFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), delivery);
    }

    for (input, kind) in [
        (
            CreateDelegationTokenInput::ResponseTooLarge,
            CreateDelegationTokenFailureKind::ResponseTooLarge,
        ),
        (
            CreateDelegationTokenInput::InvalidResponse,
            CreateDelegationTokenFailureKind::InvalidResponse,
        ),
    ] {
        let failure = failure(effect(&mut submitted_machine(), input));
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    }
}

#[test]
fn successful_response_produces_complete_token_with_request_ordered_renewers() {
    let terminal = effect(
        &mut submitted_machine(),
        CreateDelegationTokenInput::BrokerResponded {
            response: response(principal("User", "alice")),
        },
    );
    let CreateDelegationTokenEffect::Complete {
        operation_id,
        terminal: CreateDelegationTokenTerminal::Created(success),
    } = terminal
    else {
        panic!("created terminal expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(38));
    assert_eq!(success.throttle_time_ms(), 17);
    let token = success.token();
    assert_eq!(token.owner().principal_name(), "alice");
    assert_eq!(
        token
            .requester()
            .map(DelegationTokenPrincipal::principal_name),
        Some("operator")
    );
    assert_eq!(
        token
            .renewers()
            .iter()
            .map(DelegationTokenPrincipal::principal_name)
            .collect::<Vec<_>>(),
        vec!["bob", "billing"]
    );
    assert_eq!(
        (
            token.issue_timestamp_ms(),
            token.expiry_timestamp_ms(),
            token.max_timestamp_ms(),
        ),
        (100, 200, 300)
    );
    assert_eq!(token.token_id(), "token-1");
    assert_eq!(token.hmac().as_bytes(), &[1, 2, 3, 4]);
}

#[test]
fn explicit_owner_mismatch_is_a_possibly_sent_invalid_response() {
    let failure = failure(effect(
        &mut submitted_machine(),
        CreateDelegationTokenInput::BrokerResponded {
            response: response(principal("User", "mallory")),
        },
    ));

    assert_eq!(
        failure.kind(),
        CreateDelegationTokenFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn exact_signed_broker_rejection_is_a_distinct_terminal() {
    let error = CreateDelegationTokenBrokerError::new(
        23,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(
        effect(
            &mut submitted_machine(),
            CreateDelegationTokenInput::BrokerRejected { error },
        ),
        CreateDelegationTokenEffect::Complete {
            operation_id: OperationId::from_raw(38),
            terminal: CreateDelegationTokenTerminal::BrokerRejected(error),
        }
    );
}

#[test]
fn every_fact_is_stage_fenced_and_completion_is_final() {
    assert_eq!(
        machine().apply(CreateDelegationTokenInput::DriverAccepted),
        Err(CreateDelegationTokenMachineError::InvalidState)
    );
    assert_eq!(
        submitted_machine().apply(CreateDelegationTokenInput::DriverRejected),
        Err(CreateDelegationTokenMachineError::InvalidState)
    );

    let mut completed = machine();
    effect(
        &mut completed,
        CreateDelegationTokenInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(CreateDelegationTokenInput::InvalidResponse),
        Err(CreateDelegationTokenMachineError::AlreadyCompleted)
    );
}

fn plan() -> CreateDelegationTokenPlan {
    CreateDelegationTokenPlan::new(
        Some(principal("User", "alice")),
        vec![principal("User", "bob"), principal("Service", "billing")],
        Some(86_400_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine() -> CreateDelegationTokenMachine {
    CreateDelegationTokenMachine::new(OperationId::from_raw(38), Deadline::from_tick(100), plan())
}

fn awaiting_machine() -> CreateDelegationTokenMachine {
    let mut machine = machine();
    effect(
        &mut machine,
        CreateDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted_machine() -> CreateDelegationTokenMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(CreateDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accepted: {error}"));
    machine
}

fn response(owner: DelegationTokenPrincipal) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse::new(
        17,
        owner,
        Some(principal("User", "operator")),
        100,
        200,
        300,
        "token-1".to_owned(),
        DelegationTokenHmac::new(vec![1, 2, 3, 4]).unwrap_or_else(|error| panic!("hmac: {error}")),
    )
    .unwrap_or_else(|error| panic!("response: {error}"))
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}

fn effect(
    machine: &mut CreateDelegationTokenMachine,
    input: CreateDelegationTokenInput,
) -> CreateDelegationTokenEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn failure(effect: CreateDelegationTokenEffect) -> CreateDelegationTokenFailure {
    match effect {
        CreateDelegationTokenEffect::Complete {
            terminal: CreateDelegationTokenTerminal::Failed(failure),
            ..
        } => failure,
        other => panic!("expected failure, got {other:?}"),
    }
}
