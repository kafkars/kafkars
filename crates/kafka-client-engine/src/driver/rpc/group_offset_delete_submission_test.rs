//! Group-coordinator route and exact v0 request-option scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::{
    group_coordinator_route::group_coordinator_route,
    group_offset_delete_submission::group_offset_delete_options,
};

#[test]
fn route_is_the_existing_validated_group_coordinator_authority() {
    let route = group_coordinator_route("readers")
        .unwrap_or_else(|error| panic!("valid group route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("offset deletion must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");
}

#[test]
fn request_uses_one_absolute_deadline_and_exact_v0_bounds() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = group_offset_delete_options(deadline);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);
}
