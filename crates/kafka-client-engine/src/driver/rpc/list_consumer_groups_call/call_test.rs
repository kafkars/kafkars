//! Completion-fault, recovery, and synchronous-rejection correlation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::AdminGroupListingFilters;
use kafka_driver::CompletionError;

use crate::{EngineConfig, driver::DriverOwner};

use super::ListConsumerGroupsCall;

#[test]
fn discovery_completion_fault_retains_exact_recoverable_attempt() {
    let driver = driver();
    let mut call = ListConsumerGroupsCall::submit_discovery(&driver, deadline())
        .unwrap_or_else(|_error| panic!("accepted discovery call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.matches_discovery());
    assert!(call.recover_after_driver_shutdown());
    assert!(call.is_recovered());
    call.seal_recovered();
}

#[test]
fn broker_completion_fault_retains_exact_broker_filters_and_limit() {
    let driver = driver();
    let expected = filters();
    let mut call =
        ListConsumerGroupsCall::submit_broker(&driver, 7, expected.clone(), 4_096, deadline())
            .unwrap_or_else(|_error| panic!("accepted broker call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.matches_broker(7, &expected, 4_096));
    assert!(call.recover_after_driver_shutdown());
    assert!(call.matches_broker(7, &expected, 4_096));
    call.seal_recovered();
}

#[test]
fn synchronous_request_rejection_returns_exact_broker_correlation() {
    let driver = driver();
    let expected = filters();
    let rejection =
        match ListConsumerGroupsCall::submit_broker(&driver, 7, expected.clone(), 0, deadline()) {
            Ok(_call) => panic!("zero retained request limit must reject"),
            Err(rejection) => rejection,
        };

    assert_eq!(rejection.into_correlation(), (7, expected, 0));
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"))
}

fn filters() -> AdminGroupListingFilters {
    AdminGroupListingFilters::new(
        vec!["Stable".to_owned()],
        vec!["consumer".to_owned()],
        vec!["consumer".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid filters: {error}"))
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(1)
}
