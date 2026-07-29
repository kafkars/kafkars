//! Any-broker route, deadline, lane, and exact generated version window.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasPlan,
};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DescribeClientQuotasCall, DescribeClientQuotasRawTerminal,
    describe_client_quotas_submission::{
        describe_client_quotas_options, describe_client_quotas_route,
    },
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(describe_client_quotas_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_deadline_lane_and_v0_v1_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_client_quotas_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = plan("alice");
    let expected_plan = plan.clone();
    let mut call = DescribeClientQuotasCall::submit(
        &driver,
        plan,
        4 * 1024 * 1024,
        4 * 1024 * 1024,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches(&expected_plan, 4 * 1024 * 1024, 4 * 1024 * 1024));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches(&expected_plan, 4 * 1024 * 1024, 4 * 1024 * 1024));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_exact_filter_and_capacities() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = plan("alice");
    let expected_plan = plan.clone();
    let rejection = match DescribeClientQuotasCall::submit(
        &driver,
        plan,
        0,
        8_192,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero request capacity must reject"),
        Err(rejection) => rejection,
    };

    assert_eq!(rejection.into_correlation(), (expected_plan, 0, 8_192));
}

#[test]
fn raw_terminal_matches_only_the_exact_filter_and_capacities() {
    let filter_plan = plan("alice");
    let raw = DescribeClientQuotasRawTerminal::for_test(filter_plan.clone(), 4_096, 8_192);

    assert!(raw.matches(&filter_plan, 4_096, 8_192));
    assert!(!raw.matches(&plan("bob"), 4_096, 8_192));
    assert!(!raw.matches(&filter_plan, 4_095, 8_192));
    assert!(!raw.matches(&filter_plan, 4_096, 8_191));
    raw.discard();
}

fn plan(name: &str) -> DescribeClientQuotasPlan {
    DescribeClientQuotasPlan::new(
        vec![DescribeClientQuotaFilterComponent::new(
            "user".to_owned(),
            ClientQuotaMatch::Exact(name.to_owned()),
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid filter: {error}"))
}
