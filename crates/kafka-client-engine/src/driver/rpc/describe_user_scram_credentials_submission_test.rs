//! Any-broker route, deadline, lane, and exact generated v0 window.

use std::time::{Duration, Instant};

use kafka_client_core::DescribeUserScramCredentialsPlan;
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DescribeUserScramCredentialsCall, DescribeUserScramCredentialsRawTerminal,
    describe_user_scram_credentials_submission::{
        describe_user_scram_credentials_options, describe_user_scram_credentials_route,
    },
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(describe_user_scram_credentials_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_deadline_lane_and_exact_v0_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_user_scram_credentials_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn completion_fault_retains_call_and_exact_selection_until_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan =
        DescribeUserScramCredentialsPlan::new(Some(vec!["zed".to_owned(), "alice".to_owned()]))
            .unwrap_or_else(|error| panic!("valid user selection: {error}"));
    let request_limit = 4 * 1024 * 1024;
    let result_limit = request_limit - 1;
    let mut call = DescribeUserScramCredentialsCall::submit(
        &driver,
        plan.clone(),
        request_limit,
        result_limit,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches_evidence(&plan, request_limit, result_limit));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain call and selection ownership"));
    assert_eq!(recovered.plan(), &plan);
    assert!(recovered.matches_evidence(&plan, request_limit, result_limit));
    assert!(!recovered.matches_evidence(&plan, request_limit - 1, result_limit));
    recovered.seal();
}

#[test]
fn request_rejection_returns_exact_order_and_independent_bounds() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan =
        DescribeUserScramCredentialsPlan::new(Some(vec!["zed".to_owned(), "alice".to_owned()]))
            .unwrap_or_else(|error| panic!("valid user selection: {error}"));
    let rejection = match DescribeUserScramCredentialsCall::submit(
        &driver,
        plan.clone(),
        0,
        8_192,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero request bytes must reject selected users"),
        Err(rejection) => rejection,
    };

    let (returned_plan, request_limit, result_limit) = rejection.into_evidence();
    assert_eq!(returned_plan, plan);
    assert_eq!(returned_plan.users(), plan.users());
    assert_eq!((request_limit, result_limit), (0, 8_192));
}

#[test]
fn raw_evidence_distinguishes_all_users_order_and_each_bound() {
    let all = DescribeUserScramCredentialsPlan::new(None)
        .unwrap_or_else(|error| panic!("all-user selection: {error}"));
    let selected =
        DescribeUserScramCredentialsPlan::new(Some(vec!["zed".to_owned(), "alice".to_owned()]))
            .unwrap_or_else(|error| panic!("selected users: {error}"));
    let reversed =
        DescribeUserScramCredentialsPlan::new(Some(vec!["alice".to_owned(), "zed".to_owned()]))
            .unwrap_or_else(|error| panic!("reversed users: {error}"));
    let raw = DescribeUserScramCredentialsRawTerminal::for_test(selected.clone(), 4_096, 8_192);

    assert!(raw.matches_evidence(&selected, 4_096, 8_192));
    assert!(!raw.matches_evidence(&all, 4_096, 8_192));
    assert!(!raw.matches_evidence(&reversed, 4_096, 8_192));
    assert!(!raw.matches_evidence(&selected, 4_095, 8_192));
    assert!(!raw.matches_evidence(&selected, 4_096, 8_191));
}
