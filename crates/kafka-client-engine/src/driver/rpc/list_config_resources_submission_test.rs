//! AnyBroker route, original deadline, lane, and exact-v1 submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::list_config_resources_submission::{
    list_config_resources_options, list_config_resources_route,
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
