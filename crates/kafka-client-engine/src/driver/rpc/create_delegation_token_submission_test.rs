//! `AnyBroker` route, original deadline, Interactive lane, and owner-aware floor.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{
    EngineConfig,
    driver::DriverOwner,
    protocol::admin::create_delegation_token::{
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, CreateDelegationTokenRequestRef,
        DelegationTokenPrincipalRef, create_delegation_token_request,
    },
};

use super::{
    create_delegation_token_call::CreateDelegationTokenCall,
    create_delegation_token_submission::{
        create_delegation_token_minimum_version, create_delegation_token_options,
        create_delegation_token_route,
    },
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(create_delegation_token_route(), Route::AnyBroker);
}

#[test]
fn default_owner_preserves_the_v1_through_v3_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let minimum = create_delegation_token_minimum_version(1);
    let options = create_delegation_token_options(deadline, minimum);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn explicit_owner_raises_the_floor_to_v3_without_a_retry_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let minimum = create_delegation_token_minimum_version(3);
    let options = create_delegation_token_options(deadline, minimum);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = CreateDelegationTokenCall::submit(
        &driver,
        prepared(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}

fn prepared()
-> crate::protocol::admin::create_delegation_token::PreparedCreateDelegationTokenRequest {
    let owner = DelegationTokenPrincipalRef::new("User", "owner");
    let renewers = [DelegationTokenPrincipalRef::new("User", "renewer")];
    create_delegation_token_request(
        CreateDelegationTokenRequestRef::new(Some(owner), &renewers, 60_000),
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepared request: {error:?}"))
}
