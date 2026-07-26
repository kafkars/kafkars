//! Group-coordinator route and exact request-version option scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::{
    group_coordinator_route::group_coordinator_route,
    group_offsets_submission::group_offsets_options,
};

#[test]
fn route_is_the_existing_validated_group_coordinator_authority() {
    let route = group_coordinator_route("readers")
        .unwrap_or_else(|error| panic!("valid group route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("group offsets must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");
}

#[test]
fn ordinary_and_stable_queries_have_exact_version_bounds() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let ordinary = group_offsets_options(deadline, false);
    assert_eq!(ordinary.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(ordinary.maximum_version(), Some(ApiVersion::new(9)));
    assert_eq!(ordinary.traffic_class(), TrafficClass::Interactive);
    assert_eq!(ordinary.deadline(), deadline);

    let stable = group_offsets_options(deadline, true);
    assert_eq!(stable.minimum_version(), Some(ApiVersion::new(7)));
    assert_eq!(stable.maximum_version(), Some(ApiVersion::new(9)));
}
