//! Complete success, exact rejection, and secret-safe terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreateDelegationTokenBrokerError as CoreBrokerError, CreateDelegationTokenInput,
    CreateDelegationTokenMachine, CreateDelegationTokenPlan, CreateDelegationTokenResponse,
    CreateDelegationTokenTerminal, Deadline, DelegationTokenHmac, DelegationTokenPrincipal, Moment,
    OperationId,
};

use super::{CreateDelegationTokenOutcome, outcome::translate_terminal};

#[test]
fn successful_translation_moves_complete_token_and_redacts_secret() {
    let terminal = successful_terminal();
    let CreateDelegationTokenOutcome::Created(result) = translate_terminal(terminal) else {
        panic!("created token expected");
    };
    assert_eq!(result.throttle_time_ms(), 7);
    let token = result.token();
    assert_eq!(token.owner().principal_name(), "owner");
    assert_eq!(
        token.requester().map(|value| value.principal_name()),
        Some("requester")
    );
    assert_eq!(token.renewers()[0].principal_name(), "renewer");
    assert_eq!(
        (token.issue_timestamp_ms(), token.expiry_timestamp_ms()),
        (10, 20)
    );
    assert_eq!(token.max_timestamp_ms(), 30);
    assert_eq!(token.token_id(), "token-id");
    assert_eq!(token.hmac().as_bytes(), b"secret-hmac");
    let debug = format!("{result:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("secret-hmac"));
}

#[test]
fn broker_rejection_preserves_exact_signed_code() {
    let error = CoreBrokerError::new(
        17,
        NonZeroI16::new(-31_234).unwrap_or_else(|| panic!("nonzero")),
    );
    let CreateDelegationTokenOutcome::BrokerRejected(error) =
        translate_terminal(CreateDelegationTokenTerminal::BrokerRejected(error))
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(error.into_parts(), (17, -31_234));
}

fn successful_terminal() -> CreateDelegationTokenTerminal {
    let owner = core_principal("owner");
    let plan =
        CreateDelegationTokenPlan::new(Some(owner.clone()), vec![core_principal("renewer")], None)
            .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut machine =
        CreateDelegationTokenMachine::new(OperationId::from_raw(1), Deadline::from_tick(100), plan);
    let _submit = machine
        .apply(CreateDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(CreateDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let response = CreateDelegationTokenResponse::new(
        7,
        owner,
        Some(core_principal("requester")),
        10,
        20,
        30,
        "token-id".to_owned(),
        DelegationTokenHmac::new(b"secret-hmac".to_vec())
            .unwrap_or_else(|error| panic!("hmac: {error}")),
    )
    .unwrap_or_else(|error| panic!("response: {error}"));
    let transition = machine
        .apply(CreateDelegationTokenInput::BrokerResponded { response })
        .unwrap_or_else(|error| panic!("response transition: {error}"));
    let Some(kafka_client_core::CreateDelegationTokenEffect::Complete { terminal, .. }) =
        transition.into_effect()
    else {
        panic!("terminal expected");
    };
    terminal
}

fn core_principal(name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new("User".to_owned(), name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}
