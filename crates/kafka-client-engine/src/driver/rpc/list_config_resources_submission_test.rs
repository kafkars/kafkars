//! AnyBroker route, original deadline, lane, and exact-v1 submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{
    EngineConfig, driver::DriverOwner,
    protocol::admin::list_config_resources::list_config_resources_request,
};

use super::{
    ListConfigResourcesCall,
    list_config_resources_submission::{
        list_config_resources_options, list_config_resources_route,
    },
};

#[test]
fn query_uses_any_broker_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = list_config_resources_options(deadline);

    assert_eq!(list_config_resources_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn completion_fault_preserves_call_for_post_driver_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let request = list_config_resources_request(&[2, 64])
        .unwrap_or_else(|error| panic!("correlated request: {error:?}"));
    let mut call =
        ListConfigResourcesCall::submit(&driver, request, Instant::now() + Duration::from_secs(1))
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
