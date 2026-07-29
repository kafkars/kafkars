//! AnyBroker route, original deadline, Interactive lane, and v1-v2 window.

use std::time::{Duration, Instant};

use kafka_client_core::{ExpireDelegationTokenHmac, ExpireDelegationTokenPlan};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{
    EngineConfig,
    driver::DriverOwner,
    protocol::admin::expire_delegation_token::{
        EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, ExpireDelegationTokenRequestRef,
        expire_delegation_token_request,
    },
};

use super::{
    ExpireDelegationTokenCall,
    expire_delegation_token_submission::{
        expire_delegation_token_options, expire_delegation_token_route,
    },
};

#[test]
fn expiration_uses_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(expire_delegation_token_route(), Route::AnyBroker);
}

#[test]
fn expiration_preserves_original_deadline_and_exact_v1_v2_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = expire_delegation_token_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn completion_fault_retains_call_and_secret_correlation_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = ExpireDelegationTokenCall::submit(
        &driver,
        plan(),
        expire_delegation_token_request(
            ExpireDelegationTokenRequestRef::explicit(TOKEN_HMAC, 60_000),
            EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
        )
        .unwrap_or_else(|error| panic!("prepared request: {error:?}")),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted ownership"));
    assert!(recovered.matches_correlation_for_test(TOKEN_HMAC, Some(60_000)));
    recovered.seal();
}

const TOKEN_HMAC: &[u8] = &[0xA5, 0x5A, 0xC3, 0x3C];

fn plan() -> ExpireDelegationTokenPlan {
    let hmac = ExpireDelegationTokenHmac::new(TOKEN_HMAC.to_vec())
        .unwrap_or_else(|error| panic!("valid bounded HMAC: {error}"));
    ExpireDelegationTokenPlan::new(hmac, Some(60_000))
        .unwrap_or_else(|error| panic!("valid expiration plan: {error}"))
}
