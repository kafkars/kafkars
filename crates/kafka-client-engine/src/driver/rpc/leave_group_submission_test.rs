//! `LeaveGroup` route, lane, version-window, and absolute-deadline scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;
use kafka_driver::TrafficClass;
use kafka_wire::{LEAVE_GROUP_API_DESCRIPTOR, LeaveGroupRequest};

use crate::clock::OperationDeadline;

use super::leave_group_submission::{
    LEAVE_GROUP_MAX_VERSION, LEAVE_GROUP_MIN_VERSION, LeaveGroupSubmitError,
    STATIC_LEAVE_GROUP_VERSION, leave_group_options, leave_group_route,
};

#[test]
fn options_preserve_deadline_control_lane_and_dynamic_member_window() {
    let instant = Instant::now() + Duration::from_secs(7);
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(41), instant);
    let options = leave_group_options(deadline, false);

    assert_eq!(options.deadline(), instant);
    assert_eq!(options.traffic_class(), TrafficClass::Control);
    assert_eq!(options.minimum_version(), Some(LEAVE_GROUP_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(LEAVE_GROUP_MAX_VERSION));
    assert!(
        LEAVE_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(LEAVE_GROUP_MIN_VERSION)
    );
    assert!(
        LEAVE_GROUP_API_DESCRIPTOR
            .supported_versions
            .contains(LEAVE_GROUP_MAX_VERSION)
    );
}

#[test]
fn static_membership_uses_the_exact_v3_window() {
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(41), Instant::now());
    let options = leave_group_options(deadline, true);
    assert_eq!(options.minimum_version(), Some(STATIC_LEAVE_GROUP_VERSION));
    assert_eq!(options.maximum_version(), Some(STATIC_LEAVE_GROUP_VERSION));
}

#[test]
fn coordinator_and_generated_request_group_spellings_must_match() {
    let mut request = LeaveGroupRequest::default();
    request.group_id = "group-b".into();

    assert!(matches!(
        leave_group_route("group-a", &request),
        Err(LeaveGroupSubmitError::GroupMismatch)
    ));
    assert!(leave_group_route("group-b", &request).is_ok());
}
