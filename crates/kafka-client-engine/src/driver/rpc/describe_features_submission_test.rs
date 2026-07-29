//! Route, deadline, traffic, and feature-bearing-version submission evidence.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_features_submission::{describe_features_options, describe_features_route};

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
