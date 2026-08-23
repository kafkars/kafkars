//! Share coordinator, absolute deadline, and exact v1 negotiation evidence.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;
use kafka_driver::{CoordinatorKind, Route, TrafficClass};
use kafka_wire::{SHARE_GROUP_HEARTBEAT_API_DESCRIPTOR, ShareGroupHeartbeatRequest};

use crate::clock::OperationDeadline;

use super::submission::{
    SHARE_HEARTBEAT_MAX_VERSION, SHARE_HEARTBEAT_MIN_VERSION, ShareGroupHeartbeatSubmitError,
    share_group_heartbeat_options, share_group_heartbeat_route,
};

#[test]
fn options_preserve_absolute_deadline_control_lane_and_exact_v1_window() {
    let transport = Instant::now() + Duration::from_secs(5);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(50), transport);
    let options = share_group_heartbeat_options(deadline);
    assert_eq!(options.deadline(), transport);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert_eq!(options.minimum_version(), Some(SHARE_HEARTBEAT_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(SHARE_HEARTBEAT_MAX_VERSION));
    let supported = SHARE_GROUP_HEARTBEAT_API_DESCRIPTOR.supported_versions;
    assert!(supported.contains(SHARE_HEARTBEAT_MIN_VERSION));
    assert!(supported.contains(SHARE_HEARTBEAT_MAX_VERSION));
    assert_eq!(SHARE_HEARTBEAT_MIN_VERSION.value(), 1);
    assert_eq!(SHARE_HEARTBEAT_MAX_VERSION.value(), 1);
}

#[test]
fn route_uses_share_namespace_and_requires_matching_group() {
    let mut request = ShareGroupHeartbeatRequest::default();
    request.group_id = "workers".into();
    assert!(matches!(
        share_group_heartbeat_route("other", &request),
        Err(ShareGroupHeartbeatSubmitError::GroupMismatch)
    ));
    let route = share_group_heartbeat_route("workers", &request)
        .unwrap_or_else(|error| panic!("route failed: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("expected coordinator route")
    };
    assert_eq!(key.kind(), CoordinatorKind::Share);
    assert_eq!(key.as_str(), "workers");
}
