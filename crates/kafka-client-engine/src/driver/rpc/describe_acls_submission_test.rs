//! Any-broker route, deadline, lane, and version-window scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DescribeAclsFilter, DescribeAclsPlan};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DescribeAclsCall,
    describe_acls_submission::{describe_acls_options, describe_acls_route},
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(describe_acls_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_deadline_lane_and_exact_generated_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_acls_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan("orders");
    let mut call = DescribeAclsCall::submit(
        &driver,
        expected.clone(),
        4_096,
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
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches(&expected, 4_096));
    recovered.seal();
}

#[test]
fn synchronous_request_rejection_returns_exact_filter_and_limit() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan("payments");
    let Err(rejection) = DescribeAclsCall::submit(
        &driver,
        expected.clone(),
        1,
        Instant::now() + Duration::from_secs(1),
    ) else {
        panic!("insufficient request capacity must reject before tracked acceptance");
    };
    assert_eq!(rejection.into_correlation(), (expected, 1));
}

fn plan(resource_name: &str) -> DescribeAclsPlan {
    DescribeAclsPlan::new(DescribeAclsFilter::new(
        2,
        Some(resource_name.to_owned()),
        3,
        None,
        None,
        1,
        1,
    ))
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
