//! Submission-policy tests for Admin `ListClientMetricsResources`.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    ListClientMetricsResourcesCall,
    list_client_metrics_resources_submission::{
        list_client_metrics_resources_options, list_client_metrics_resources_route,
    },
};

#[test]
fn request_uses_any_broker_and_preserves_the_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = list_client_metrics_resources_options(deadline);

    assert_eq!(list_client_metrics_resources_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
}

#[test]
fn request_is_pinned_to_exactly_version_zero() {
    let options = list_client_metrics_resources_options(Instant::now());

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call =
        ListClientMetricsResourcesCall::submit(&driver, Instant::now() + Duration::from_secs(1))
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
