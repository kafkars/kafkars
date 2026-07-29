//! Any-broker route, deadline, lane, and exact generated v0 window.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::alter_user_scram_credentials_submission::{
    alter_user_scram_credentials_options, alter_user_scram_credentials_route,
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(alter_user_scram_credentials_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = alter_user_scram_credentials_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
