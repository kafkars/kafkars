//! Deadline, ordering, correlation, delivery, and terminal scenarios.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact effect ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    super::{DelegationTokenHmac, DelegationTokenPrincipal},
    DescribeDelegationTokenResponse, DescribeDelegationTokensBrokerError,
    DescribeDelegationTokensEffect, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensInput, DescribeDelegationTokensMachine,
    DescribeDelegationTokensMachineError, DescribeDelegationTokensPlan,
    DescribeDelegationTokensResponse, DescribeDelegationTokensState,
    DescribeDelegationTokensTerminal,
};

#[test]
fn sole_submission_reuses_identity_deadline_and_exact_selection() {
    let plan = filtered_plan();
    let mut machine = machine(plan.clone());
    let submit = effect(
        &mut machine,
        DescribeDelegationTokensInput::Start {
            now: Moment::from_tick(1),
        },
    );

    assert_eq!(
        submit,
        DescribeDelegationTokensEffect::Submit {
            operation_id: OperationId::from_raw(41),
            deadline: Deadline::from_tick(100),
            plan,
        }
    );
    assert_eq!(
        machine.state(),
        DescribeDelegationTokensState::AwaitingDriver
    );
    accept(&mut machine);
    assert_eq!(machine.state(), DescribeDelegationTokensState::Submitted);
}

#[test]
fn filtered_results_restore_owner_then_token_id_order_and_complete_facts() {
    let mut machine = submitted(filtered_plan());
    let response = DescribeDelegationTokensResponse::new(
        29,
        vec![
            token("User", "alice", "zeta", Some("requester"), b"secret-z"),
            token("Service", "billing", "middle", None, b"secret-m"),
            token("User", "alice", "alpha", None, b"secret-a"),
        ],
    )
    .unwrap_or_else(|error| panic!("response: {error}"));
    let terminal = effect(
        &mut machine,
        DescribeDelegationTokensInput::BrokerResponded { response },
    );
    let DescribeDelegationTokensEffect::Complete {
        terminal: DescribeDelegationTokensTerminal::Described(listing),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };

    assert_eq!(listing.throttle_time_ms(), 29);
    let tokens = listing.tokens();
    assert_eq!(
        tokens
            .iter()
            .map(|token| (token.owner().principal_name(), token.token_id()))
            .collect::<Vec<_>>(),
        [("billing", "middle"), ("alice", "alpha"), ("alice", "zeta"),]
    );
    assert_eq!(
        tokens[2]
            .requester()
            .map(DelegationTokenPrincipal::principal_name),
        Some("requester")
    );
    assert_eq!(tokens[2].renewers()[0].principal_name(), "renewer");
    assert_eq!(
        (
            tokens[2].issue_timestamp_ms(),
            tokens[2].expiry_timestamp_ms(),
            tokens[2].max_timestamp_ms(),
        ),
        (10, 20, 30)
    );
    assert_eq!(tokens[2].hmac().as_bytes(), b"secret-z");
    assert!(!format!("{listing:?}").contains("secret-z"));
}

#[test]
fn all_results_use_principal_type_name_then_token_id_byte_order() {
    let mut machine = submitted(DescribeDelegationTokensPlan::all());
    let response = DescribeDelegationTokensResponse::new(
        0,
        vec![
            token("User", "bob", "z", None, b"z"),
            token("Service", "bob", "x", None, b"x"),
            token("User", "alice", "b", None, b"b"),
            token("User", "alice", "a", None, b"a"),
        ],
    )
    .unwrap_or_else(|error| panic!("response: {error}"));
    let terminal = effect(
        &mut machine,
        DescribeDelegationTokensInput::BrokerResponded { response },
    );
    let DescribeDelegationTokensEffect::Complete {
        terminal: DescribeDelegationTokensTerminal::Described(listing),
        ..
    } = terminal
    else {
        panic!("described terminal expected");
    };
    assert_eq!(
        listing
            .tokens()
            .iter()
            .map(|token| (
                token.owner().principal_type(),
                token.owner().principal_name(),
                token.token_id(),
            ))
            .collect::<Vec<_>>(),
        [
            ("Service", "bob", "x"),
            ("User", "alice", "a"),
            ("User", "alice", "b"),
            ("User", "bob", "z"),
        ]
    );
}

#[test]
fn foreign_owner_and_duplicate_token_identity_are_invalid_response() {
    let foreign = response(vec![token("User", "mallory", "one", None, b"one")]);
    assert_invalid_response(effect(
        &mut submitted(filtered_plan()),
        DescribeDelegationTokensInput::BrokerResponded { response: foreign },
    ));

    let duplicates = response(vec![
        token("Service", "billing", "same", None, b"one"),
        token("User", "alice", "same", None, b"two"),
    ]);
    assert_invalid_response(effect(
        &mut submitted(filtered_plan()),
        DescribeDelegationTokensInput::BrokerResponded {
            response: duplicates,
        },
    ));
}

#[test]
fn deadline_delivery_exact_rejection_and_stage_fences_are_terminal_once() {
    assert_failure(
        effect(
            &mut machine(DescribeDelegationTokensPlan::all()),
            DescribeDelegationTokensInput::Start {
                now: Moment::from_tick(100),
            },
        ),
        DescribeDelegationTokensFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
    assert_failure(
        effect(
            &mut awaiting(DescribeDelegationTokensPlan::all()),
            DescribeDelegationTokensInput::DriverRejected,
        ),
        DescribeDelegationTokensFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
    assert_failure(
        effect(
            &mut submitted(DescribeDelegationTokensPlan::all()),
            DescribeDelegationTokensInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        ),
        DescribeDelegationTokensFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    for (input, kind, delivery) in [
        (
            DescribeDelegationTokensInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeDelegationTokensFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeDelegationTokensInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeDelegationTokensFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeDelegationTokensInput::ResponseTooLarge,
            DescribeDelegationTokensFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeDelegationTokensInput::InvalidResponse,
            DescribeDelegationTokensFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        assert_failure(
            effect(&mut submitted(DescribeDelegationTokensPlan::all()), input),
            kind,
            delivery,
        );
    }

    let error = DescribeDelegationTokensBrokerError::new(
        31,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero")),
    );
    assert_eq!(
        effect(
            &mut submitted(DescribeDelegationTokensPlan::all()),
            DescribeDelegationTokensInput::BrokerRejected { error },
        ),
        DescribeDelegationTokensEffect::Complete {
            operation_id: OperationId::from_raw(41),
            terminal: DescribeDelegationTokensTerminal::BrokerRejected(error),
        }
    );

    let mut completed = machine(DescribeDelegationTokensPlan::all());
    let _ = effect(
        &mut completed,
        DescribeDelegationTokensInput::Start {
            now: Moment::from_tick(100),
        },
    );
    assert_eq!(
        completed.apply(DescribeDelegationTokensInput::InvalidResponse),
        Err(DescribeDelegationTokensMachineError::AlreadyCompleted)
    );
}

fn filtered_plan() -> DescribeDelegationTokensPlan {
    DescribeDelegationTokensPlan::for_owners(vec![
        principal("Service", "billing"),
        principal("User", "alice"),
    ])
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn machine(plan: DescribeDelegationTokensPlan) -> DescribeDelegationTokensMachine {
    DescribeDelegationTokensMachine::new(OperationId::from_raw(41), Deadline::from_tick(100), plan)
}

fn awaiting(plan: DescribeDelegationTokensPlan) -> DescribeDelegationTokensMachine {
    let mut machine = machine(plan);
    let _ = effect(
        &mut machine,
        DescribeDelegationTokensInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
}

fn submitted(plan: DescribeDelegationTokensPlan) -> DescribeDelegationTokensMachine {
    let mut machine = awaiting(plan);
    accept(&mut machine);
    machine
}

fn accept(machine: &mut DescribeDelegationTokensMachine) {
    assert!(
        machine
            .apply(DescribeDelegationTokensInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accepted: {error}"))
            .into_effect()
            .is_none()
    );
}

fn response(tokens: Vec<DescribeDelegationTokenResponse>) -> DescribeDelegationTokensResponse {
    DescribeDelegationTokensResponse::new(0, tokens)
        .unwrap_or_else(|error| panic!("response: {error}"))
}

fn token(
    principal_type: &str,
    principal_name: &str,
    token_id: &str,
    requester: Option<&str>,
    secret: &[u8],
) -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse::new(
        principal(principal_type, principal_name),
        requester.map(|name| principal("User", name)),
        vec![principal("Service", "renewer")],
        10,
        20,
        30,
        token_id.to_owned(),
        DelegationTokenHmac::new(secret.to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
    )
    .unwrap_or_else(|error| panic!("token: {error}"))
}

fn principal(principal_type: &str, principal_name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new(principal_type.to_owned(), principal_name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}

fn effect(
    machine: &mut DescribeDelegationTokensMachine,
    input: DescribeDelegationTokensInput,
) -> DescribeDelegationTokensEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect"))
}

fn assert_invalid_response(effect: DescribeDelegationTokensEffect) {
    assert_failure(
        effect,
        DescribeDelegationTokensFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

fn assert_failure(
    effect: DescribeDelegationTokensEffect,
    expected_kind: DescribeDelegationTokensFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let DescribeDelegationTokensEffect::Complete {
        terminal: DescribeDelegationTokensTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("failure terminal expected");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
