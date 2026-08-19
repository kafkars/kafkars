//! Complete listing, exact rejection, and secret-safe terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DelegationTokenHmac, DelegationTokenPrincipal, DescribeDelegationTokenResponse,
    DescribeDelegationTokensBrokerError as CoreBrokerError, DescribeDelegationTokensEffect,
    DescribeDelegationTokensInput, DescribeDelegationTokensMachine, DescribeDelegationTokensPlan,
    DescribeDelegationTokensResponse, DescribeDelegationTokensTerminal, Moment, OperationId,
};

use super::{DescribeDelegationTokensOutcome, outcome::translate_terminal};

#[test]
fn successful_translation_moves_every_complete_token_and_redacts_secrets() {
    let terminal = successful_terminal();
    let DescribeDelegationTokensOutcome::Described(result) = translate_terminal(terminal) else {
        panic!("described tokens expected");
    };
    assert_eq!(result.throttle_time_ms(), 7);
    assert_eq!(result.tokens().len(), 2);
    let token = &result.tokens()[0];
    assert_eq!(token.owner().principal_name(), "alice");
    assert_eq!(
        token
            .requester()
            .map(super::model::DescribeDelegationTokenPrincipal::principal_name),
        Some("requester")
    );
    assert_eq!(token.renewers()[0].principal_name(), "renewer");
    assert_eq!(
        (token.issue_timestamp_ms(), token.expiry_timestamp_ms()),
        (10, 20)
    );
    assert_eq!(token.max_timestamp_ms(), 30);
    assert_eq!(token.token_id(), "a-token");
    assert_eq!(token.hmac().as_bytes(), b"secret-a");
    let debug = format!("{result:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("secret-a"));
}

#[test]
fn broker_rejection_preserves_throttle_and_exact_signed_code() {
    let error = CoreBrokerError::new(
        17,
        NonZeroI16::new(-31_234).unwrap_or_else(|| panic!("nonzero")),
    );
    let DescribeDelegationTokensOutcome::BrokerRejected(error) =
        translate_terminal(DescribeDelegationTokensTerminal::BrokerRejected(error))
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(error.into_parts(), (17, -31_234));
}

fn successful_terminal() -> DescribeDelegationTokensTerminal {
    let plan = DescribeDelegationTokensPlan::all();
    let mut machine = DescribeDelegationTokensMachine::new(
        OperationId::from_raw(41),
        Deadline::from_tick(100),
        plan,
    );
    let _submit = machine
        .apply(DescribeDelegationTokensInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(DescribeDelegationTokensInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let response = DescribeDelegationTokensResponse::new(
        7,
        vec![
            token("bob", "b-token", None, b"secret-b"),
            token("alice", "a-token", Some("requester"), b"secret-a"),
        ],
    )
    .unwrap_or_else(|error| panic!("response: {error}"));
    let transition = machine
        .apply(DescribeDelegationTokensInput::BrokerResponded { response })
        .unwrap_or_else(|error| panic!("response transition: {error}"));
    let Some(DescribeDelegationTokensEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal expected");
    };
    terminal
}

fn token(
    owner: &str,
    token_id: &str,
    requester: Option<&str>,
    secret: &[u8],
) -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse::new(
        principal(owner),
        requester.map(principal),
        vec![
            DelegationTokenPrincipal::new("Service".to_owned(), "renewer".to_owned())
                .unwrap_or_else(|error| panic!("renewer: {error}")),
        ],
        10,
        20,
        30,
        token_id.to_owned(),
        DelegationTokenHmac::new(secret.to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
    )
    .unwrap_or_else(|error| panic!("token: {error}"))
}

fn principal(name: &str) -> DelegationTokenPrincipal {
    DelegationTokenPrincipal::new("User".to_owned(), name.to_owned())
        .unwrap_or_else(|error| panic!("principal: {error}"))
}
