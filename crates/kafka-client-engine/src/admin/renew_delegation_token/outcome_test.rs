//! Successful renewal and exact broker-rejection terminal translation.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, Moment, OperationId, RenewDelegationTokenBrokerError as CoreBrokerError,
    RenewDelegationTokenEffect, RenewDelegationTokenHmac as CoreHmac, RenewDelegationTokenInput,
    RenewDelegationTokenMachine, RenewDelegationTokenPlan, RenewDelegationTokenResponse,
    RenewDelegationTokenTerminal,
};

use super::{RenewDelegationTokenOutcome, outcome::translate_terminal};

#[test]
fn successful_translation_preserves_throttle_and_expiry() {
    let RenewDelegationTokenOutcome::Renewed(result) = translate_terminal(successful_terminal())
    else {
        panic!("renewal success expected");
    };

    assert_eq!(result.into_parts(), (7, 1_700_003_600_002));
}

#[test]
fn broker_rejection_preserves_exact_signed_code() {
    let error = CoreBrokerError::new(
        17,
        NonZeroI16::new(-31_234).unwrap_or_else(|| panic!("nonzero")),
    );
    let RenewDelegationTokenOutcome::BrokerRejected(error) =
        translate_terminal(RenewDelegationTokenTerminal::BrokerRejected(error))
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.into_parts(), (17, -31_234));
}

fn successful_terminal() -> RenewDelegationTokenTerminal {
    let mut machine = RenewDelegationTokenMachine::new(
        OperationId::from_raw(39),
        Deadline::from_tick(100),
        plan(),
    );
    let _submission = machine
        .apply(RenewDelegationTokenInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));
    machine
        .apply(RenewDelegationTokenInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let response = RenewDelegationTokenResponse::new(7, 1_700_003_600_002)
        .unwrap_or_else(|error| panic!("response: {error}"));
    let transition = machine
        .apply(RenewDelegationTokenInput::BrokerResponded { response })
        .unwrap_or_else(|error| panic!("response transition: {error}"));
    let Some(RenewDelegationTokenEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("terminal expected");
    };
    terminal
}

fn plan() -> RenewDelegationTokenPlan {
    RenewDelegationTokenPlan::new(
        CoreHmac::new(b"renew-secret".to_vec()).unwrap_or_else(|error| panic!("hmac: {error}")),
        Some(60_000),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
