//! Deadline, delivery, terminal, and response-correlation SCRAM scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    ALTER_USER_SCRAM_CREDENTIALS_SHA_512, AlterUserScramCredentialBrokerError,
    AlterUserScramCredentialChange, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialResult, AlterUserScramCredentialsBatch,
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsFailureKind,
    AlterUserScramCredentialsInput, AlterUserScramCredentialsMachine,
    AlterUserScramCredentialsMachineError, AlterUserScramCredentialsPlan,
    AlterUserScramCredentialsState, AlterUserScramCredentialsTerminal,
    AlterUserScramCredentialsTransition,
};

#[test]
fn response_is_restored_to_unique_user_first_occurrence_order() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_111).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = AlterUserScramCredentialsBatch::new(
        73,
        vec![
            AlterUserScramCredentialOutcome::failed(
                "alice".to_owned(),
                AlterUserScramCredentialBrokerError::new(
                    code,
                    Some("not allowed".to_owned()),
                    false,
                ),
            ),
            AlterUserScramCredentialOutcome::altered("bob".to_owned()),
        ],
    );
    let transition = machine
        .apply(AlterUserScramCredentialsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("correlated response should settle: {error}"));
    let Some(AlterUserScramCredentialsEffect::Complete {
        terminal: AlterUserScramCredentialsTerminal::Altered(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("valid response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].user(), "bob");
    assert_eq!(batch.outcomes()[1].user(), "alice");
    let AlterUserScramCredentialResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("alice result must retain exact broker failure");
    };
    assert_eq!(error.code(), -32_111);
    assert_eq!(machine.state(), AlterUserScramCredentialsState::Completed);
    assert_eq!(
        machine.apply(AlterUserScramCredentialsInput::InvalidResponse),
        Err(AlterUserScramCredentialsMachineError::AlreadyCompleted)
    );
}

#[test]
fn missing_extra_duplicate_unexpected_and_malformed_users_fail_once() {
    let malformed = [
        AlterUserScramCredentialsBatch::new(
            0,
            vec![AlterUserScramCredentialOutcome::altered("bob".to_owned())],
        ),
        AlterUserScramCredentialsBatch::new(
            0,
            vec![
                AlterUserScramCredentialOutcome::altered("bob".to_owned()),
                AlterUserScramCredentialOutcome::altered("alice".to_owned()),
                AlterUserScramCredentialOutcome::altered("mallory".to_owned()),
            ],
        ),
        AlterUserScramCredentialsBatch::new(
            0,
            vec![
                AlterUserScramCredentialOutcome::altered("bob".to_owned()),
                AlterUserScramCredentialOutcome::altered("bob".to_owned()),
            ],
        ),
        AlterUserScramCredentialsBatch::new(
            0,
            vec![
                AlterUserScramCredentialOutcome::altered("bob".to_owned()),
                AlterUserScramCredentialOutcome::altered("mallory".to_owned()),
            ],
        ),
        AlterUserScramCredentialsBatch::new(
            0,
            vec![
                AlterUserScramCredentialOutcome::altered(String::new()),
                AlterUserScramCredentialOutcome::altered("alice".to_owned()),
            ],
        ),
    ];
    for batch in malformed {
        assert_invalid_response(batch);
    }
}

#[test]
fn oversized_diagnostic_is_an_invalid_possibly_sent_response() {
    let code = NonZeroI16::new(1).unwrap_or_else(|| panic!("code is nonzero"));
    assert_invalid_response(AlterUserScramCredentialsBatch::new(
        0,
        vec![
            AlterUserScramCredentialOutcome::altered("bob".to_owned()),
            AlterUserScramCredentialOutcome::failed(
                "alice".to_owned(),
                AlterUserScramCredentialBrokerError::new(
                    code,
                    Some("x".repeat(ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES + 1)),
                    false,
                ),
            ),
        ],
    ));
}

#[test]
fn pre_driver_deadline_and_rejection_are_definitely_unsent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(AlterUserScramCredentialsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        AlterUserScramCredentialsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    for input in [
        AlterUserScramCredentialsInput::DeadlineElapsed,
        AlterUserScramCredentialsInput::DriverRejected,
    ] {
        let mut awaiting = machine(20);
        awaiting
            .apply(AlterUserScramCredentialsInput::Start {
                now: Moment::from_tick(1),
            })
            .unwrap_or_else(|error| panic!("start should submit: {error}"));
        let kind = match input {
            AlterUserScramCredentialsInput::DeadlineElapsed => {
                AlterUserScramCredentialsFailureKind::DeadlineElapsed
            }
            AlterUserScramCredentialsInput::DriverRejected => {
                AlterUserScramCredentialsFailureKind::DriverRejected
            }
            _ => panic!("fixture contains only pre-driver facts"),
        };
        let transition = awaiting
            .apply(input)
            .unwrap_or_else(|error| panic!("pre-driver failure should settle: {error}"));
        assert_failure(transition, kind, DeliveryStatus::NotSent);
    }
}

#[test]
fn submitted_failures_preserve_driver_authoritative_delivery_without_retry() {
    for (input, kind, delivery) in [
        (
            AlterUserScramCredentialsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterUserScramCredentialsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterUserScramCredentialsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            AlterUserScramCredentialsFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            AlterUserScramCredentialsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterUserScramCredentialsFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterUserScramCredentialsInput::ResponseTooLarge,
            AlterUserScramCredentialsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterUserScramCredentialsInput::InvalidResponse,
            AlterUserScramCredentialsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("submitted failure should settle: {error}"));
        assert_failure(transition, kind, delivery);
        assert_eq!(machine.state(), AlterUserScramCredentialsState::Completed);
    }
}

fn assert_invalid_response(batch: AlterUserScramCredentialsBatch) {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(AlterUserScramCredentialsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
    assert_failure(
        transition,
        AlterUserScramCredentialsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

fn assert_failure(
    transition: AlterUserScramCredentialsTransition,
    kind: AlterUserScramCredentialsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AlterUserScramCredentialsEffect::Complete {
        terminal: AlterUserScramCredentialsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn submitted_machine() -> AlterUserScramCredentialsMachine {
    let mut machine = machine(20);
    machine
        .apply(AlterUserScramCredentialsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterUserScramCredentialsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn machine(deadline: u64) -> AlterUserScramCredentialsMachine {
    AlterUserScramCredentialsMachine::new(
        OperationId::from_raw(51),
        Deadline::from_tick(deadline),
        AlterUserScramCredentialsPlan::new(vec![
            AlterUserScramCredentialChange::upsertion(
                "bob".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
                8192,
            ),
            AlterUserScramCredentialChange::deletion(
                "alice".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            ),
            AlterUserScramCredentialChange::deletion(
                "bob".to_owned(),
                ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
            ),
        ])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}
