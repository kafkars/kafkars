//! Successful expiration and exact broker-rejection terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, ExpireDelegationTokenBrokerError as CoreBrokerError, ExpireDelegationTokenEffect,
    ExpireDelegationTokenHmac as CoreHmac, ExpireDelegationTokenInput,
    ExpireDelegationTokenMachine, ExpireDelegationTokenPlan, ExpireDelegationTokenResponse,
    ExpireDelegationTokenTerminal, Moment, OperationId,
};

use super::{ExpireDelegationTokenOutcome, outcome::translate_terminal};

#[test]
fn successful_translation_preserves_throttle_and_expiry() {
    let ExpireDelegationTokenOutcome::Expired(result) = translate_terminal(successful_terminal())
    else {
        panic!("expiration success expected");
    };

    assert_eq!(result.into_parts(), (7, 1_700_003_600_002));
}

#[test]
fn broker_rejection_preserves_exact_signed_code() {
    let error = CoreBrokerError::new(
        17,
        NonZeroI16::new(-31_234).unwrap_or_else(|| panic!("nonzero")),
    );
    let ExpireDelegationTokenOutcome::BrokerRejected(error) =
        translate_terminal(ExpireDelegationTokenTerminal::BrokerRejected(error))
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.into_parts(), (17, -31_234));
}

fn successful_terminal() -> ExpireDelegationTokenTerminal {
    let mut machine = ExpireDelegationTokenMachine::new(
        OperationId::from_raw(40),
        Deadline::from_tick(100),
        plan(),
    );
    let _submission = machine
        .apply(ExpireDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(ExpireDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let response = ExpireDelegationTokenResponse::new(7, 1_700_003_600_002)
        .unwrap_or_else(|error| panic!("response: {error}"));
    let transition = machine
        .apply(ExpireDelegationTokenInput::BrokerResponded { response })
        .unwrap_or_else(|error| panic!("response transition: {error}"));
    let Some(ExpireDelegationTokenEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal expected");
    };
    terminal
}

fn plan() -> ExpireDelegationTokenPlan {
    ExpireDelegationTokenPlan::new(
        CoreHmac::new(b"expire-secret".to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
        Some(60_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
