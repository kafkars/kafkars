//! Route, deadline, traffic, and feature-bearing-version submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DescribeFeaturesCall,
    describe_features_submission::{describe_features_options, describe_features_route},
};

#[test]
fn query_uses_any_broker_and_preserves_the_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_features_options(deadline);

    assert_eq!(describe_features_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(5)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeFeaturesCall::submit(&driver, Instant::now() + Duration::from_secs(1))
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
