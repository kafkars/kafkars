//! Any-broker route, deadline, lane, and exact generated version window.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_client_quotas_submission::{
    describe_client_quotas_options, describe_client_quotas_route,
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
