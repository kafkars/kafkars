//! Exact-broker route and version-window evidence for `DescribeLogDirs`.

use std::time::{Duration, Instant};

use kafka_driver::{CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::DescribeLogDirsCall;
use super::describe_log_dirs_submission::{describe_log_dirs_options, describe_log_dirs_route};

#[test]
fn route_targets_the_requested_broker() {
    assert_eq!(
        describe_log_dirs_route(17).expect("valid broker"),
        Route::AnyBroker
    );
    assert!(describe_log_dirs_route(-1).is_err());
}

#[test]
fn options_preserve_deadline_lane_and_supported_versions() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let options = describe_log_dirs_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.minimum_version().map(|version| version.value()),
        Some(1)
    );
    assert_eq!(
        options.maximum_version().map(|version| version.value()),
        Some(5)
    );
}

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeLogDirsCall::submit(&driver, 1, Instant::now() + Duration::from_secs(1))
        .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
