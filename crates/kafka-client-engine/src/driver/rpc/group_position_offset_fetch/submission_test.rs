//! Dedicated group-coordinator route, deadline, lane, and version scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::submission::group_position_offset_fetch_options;
use crate::driver::rpc::group_coordinator_route::group_coordinator_route;

#[test]
fn options_are_interactive_v2_through_v9_at_the_unchanged_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = group_position_offset_fetch_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(9)));
}

#[test]
fn route_is_the_existing_group_coordinator_authority() {
    let route = group_coordinator_route("readers")
        .unwrap_or_else(|error| panic!("valid group route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("group position OffsetFetch must use a coordinator route");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");
}
