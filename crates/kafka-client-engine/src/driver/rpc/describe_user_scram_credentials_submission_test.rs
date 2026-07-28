//! Any-broker route, deadline, lane, and exact generated v0 window.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_user_scram_credentials_submission::{
    describe_user_scram_credentials_options, describe_user_scram_credentials_route,
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
