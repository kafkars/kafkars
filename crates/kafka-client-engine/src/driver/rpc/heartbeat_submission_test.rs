//! Classic Heartbeat route and absolute-deadline submission scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;
use kafka_driver::TrafficClass;
use kafka_wire::{HEARTBEAT_API_DESCRIPTOR, HeartbeatRequest};

use crate::clock::OperationDeadline;

use super::heartbeat_submission::{
    ClassicHeartbeatSubmitError, HEARTBEAT_MAX_VERSION, HEARTBEAT_MIN_VERSION,
    STATIC_HEARTBEAT_VERSION, classic_heartbeat_options, classic_heartbeat_route,
};

#[test]
fn options_preserve_absolute_deadline_control_lane_and_version_bounds() {
    let transport = Instant::now() + Duration::from_secs(5);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(50), transport);
    let options = classic_heartbeat_options(deadline, false);

    assert_eq!(options.deadline(), transport);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert!(options.rejects_after_route_failure());
    assert_eq!(options.minimum_version(), Some(HEARTBEAT_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(HEARTBEAT_MAX_VERSION));
}

#[test]
fn static_membership_uses_the_exact_v3_window() {
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now());
    let options = classic_heartbeat_options(deadline, true);
    assert!(options.rejects_after_route_failure());
    assert_eq!(options.minimum_version(), Some(STATIC_HEARTBEAT_VERSION));
    assert_eq!(options.maximum_version(), Some(STATIC_HEARTBEAT_VERSION));
}

#[test]
fn policy_window_is_an_exact_supported_subset_of_the_wire_descriptor() {
    let supported = HEARTBEAT_API_DESCRIPTOR.supported_versions;

    assert!(supported.contains(HEARTBEAT_MIN_VERSION));
    assert!(supported.contains(HEARTBEAT_MAX_VERSION));
    assert_eq!(HEARTBEAT_MIN_VERSION.value(), 0);
    assert_eq!(HEARTBEAT_MAX_VERSION.value(), 2);
}

#[test]
fn coordinator_and_generated_request_group_spellings_must_match() {
    let mut request = HeartbeatRequest::default();
    request.group_id = "group-b".into();

    assert!(matches!(
        classic_heartbeat_route("group-a", &request),
        Err(ClassicHeartbeatSubmitError::GroupMismatch)
    ));
    assert!(classic_heartbeat_route("group-b", &request).is_ok());
}
