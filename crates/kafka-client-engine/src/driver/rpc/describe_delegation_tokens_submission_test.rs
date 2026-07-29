//! AnyBroker route, original deadline, Interactive lane, and v1-v3 window.

use std::time::{Duration, Instant};

use kafka_client_core::{DelegationTokenPrincipal, DescribeDelegationTokensPlan};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{
    EngineConfig,
    driver::DriverOwner,
    protocol::admin::describe_delegation_tokens::{
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, DescribeDelegationTokenPrincipalRef,
        DescribeDelegationTokensRequestRef, PreparedDescribeDelegationTokensRequest,
        describe_delegation_tokens_request,
    },
};

use super::{
    DescribeDelegationTokensCall,
    describe_delegation_tokens_submission::{
        describe_delegation_tokens_options, describe_delegation_tokens_route,
    },
};

#[test]
fn query_uses_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(describe_delegation_tokens_route(), Route::AnyBroker);
}

#[test]
fn query_preserves_original_deadline_and_exact_v1_v3_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_delegation_tokens_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn completion_fault_retains_call_and_exact_owner_selection_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeDelegationTokensCall::submit(
        &driver,
        prepared_owner(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown(owner_plan())
        .unwrap_or_else(|| panic!("completion fault must retain call and owner selection"));
    let kafka_client_core::DescribeDelegationTokensSelection::Owners(owners) =
        recovered.plan().selection()
    else {
        panic!("explicit owner selection expected");
    };
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].principal_type(), "User");
    assert_eq!(owners[0].principal_name(), "alice");
    recovered.seal();
}

fn owner_plan() -> DescribeDelegationTokensPlan {
    DescribeDelegationTokensPlan::for_owners(vec![
        DelegationTokenPrincipal::new("User".to_owned(), "alice".to_owned())
            .unwrap_or_else(|error| panic!("valid owner: {error}")),
    ])
    .unwrap_or_else(|error| panic!("valid owner selection: {error}"))
}

fn prepared_owner() -> PreparedDescribeDelegationTokensRequest {
    let owners = [DescribeDelegationTokenPrincipalRef::new("User", "alice")];
    describe_delegation_tokens_request(
        DescribeDelegationTokensRequestRef::selected(&owners),
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepared owner query: {error:?}"))
}
