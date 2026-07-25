//! Group-coordinator route construction scenarios.

use kafka_driver::{CoordinatorKeyError, CoordinatorKind, Route};

use super::group_coordinator_route::group_coordinator_route;

#[test]
fn route_preserves_the_exact_group_key_and_namespace() {
    let route = group_coordinator_route("readers")
        .unwrap_or_else(|error| panic!("valid group route rejected: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("classic membership requires a coordinator route");
    };

    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");
}

#[test]
fn invalid_group_is_rejected_before_driver_ownership() {
    assert_eq!(group_coordinator_route(""), Err(CoordinatorKeyError::Empty));
}
