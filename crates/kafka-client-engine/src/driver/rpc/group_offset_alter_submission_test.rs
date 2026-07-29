//! Group-coordinator route and exact name-based request-option scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};

use super::{
    group_coordinator_route::group_coordinator_route,
    group_offset_alter_submission::group_offset_alter_options,
};

#[test]
fn route_is_the_existing_validated_group_coordinator_authority() {
    let route = group_coordinator_route("readers")
        .unwrap_or_else(|error| panic!("valid group route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("offset alteration must use coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");
}

#[test]
fn request_uses_one_absolute_deadline_and_exact_name_based_bounds() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let targets = [
        crate::protocol::admin::group_offset_alter::OffsetCommitTargetRef::new(
            "orders",
            0,
            91,
            Some(7),
            None,
        ),
    ];
    let options = group_offset_alter_options(&targets, None, deadline);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(6)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(9)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);
}

#[test]
fn explicit_retention_caps_the_driver_at_v4() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let targets = [
        crate::protocol::admin::group_offset_alter::OffsetCommitTargetRef::new(
            "orders", 0, 91, None, None,
        ),
    ];
    let options = group_offset_alter_options(&targets, Some(86_400_000), deadline);

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(4)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);
}

#[test]
fn retention_and_leader_epoch_produce_invalid_driver_bounds() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let targets = [
        crate::protocol::admin::group_offset_alter::OffsetCommitTargetRef::new(
            "orders",
            0,
            91,
            Some(7),
            None,
        ),
    ];
    let options = group_offset_alter_options(&targets, Some(86_400_000), deadline);

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(6)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(4)));
}
