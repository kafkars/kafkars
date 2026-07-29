//! Any-broker route, deadline, lane, and exact generated v0 window.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, AlterUserScramCredentialChange,
    AlterUserScramCredentialsPlan,
};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{
    EngineConfig,
    driver::DriverOwner,
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
        PreparedAlterUserScramCredentialsRequest, alter_user_scram_credentials_request,
    },
};

use super::{
    AlterUserScramCredentialsCall,
    alter_user_scram_credentials_submission::{
        alter_user_scram_credentials_options, alter_user_scram_credentials_route,
    },
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(alter_user_scram_credentials_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = alter_user_scram_credentials_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn completion_fault_retains_call_and_correlation_plan_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let prepared = prepared_delete("alice");
    let prepared_request_bytes = prepared.retained_heap_bytes();
    let result_limit = 8_192;
    let mut call = AlterUserScramCredentialsCall::submit(
        &driver,
        expected.clone(),
        prepared,
        result_limit,
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
        .unwrap_or_else(|_call| panic!("completion fault must retain call and plan ownership"));
    assert!(recovered.matches_evidence(&expected, prepared_request_bytes, result_limit));
    recovered.seal();
}

#[test]
fn zero_result_capacity_rejects_and_returns_only_non_secret_correlation() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let prepared = prepared_delete("alice");
    let prepared_request_bytes = prepared.retained_heap_bytes();
    let rejection = match AlterUserScramCredentialsCall::submit(
        &driver,
        expected.clone(),
        prepared,
        0,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero terminal capacity must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(
        rejection.into_correlation(),
        (expected, prepared_request_bytes, 0)
    );
}

fn plan() -> AlterUserScramCredentialsPlan {
    AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
        "alice".to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )])
    .unwrap_or_else(|error| panic!("valid deletion plan: {error}"))
}

fn prepared_delete(user: &str) -> PreparedAlterUserScramCredentialsRequest {
    let alterations = [AlterUserScramCredentialAlterationRef::delete(
        user,
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )];
    alter_user_scram_credentials_request(
        AlterUserScramCredentialsRequestRef::new(&alterations),
        4 * 1024 * 1024,
    )
    .unwrap_or_else(|error| panic!("prepare deletion: {error:?}"))
}
