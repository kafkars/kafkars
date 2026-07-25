//! `JoinGroup` route and absolute-deadline submission scenarios.

use std::time::Instant;

use kafka_driver::TrafficClass;
use kafka_wire::{JOIN_GROUP_API_DESCRIPTOR, JoinGroupRequest};

use super::join_group_submission::{
    JOIN_GROUP_MAX_VERSION, JOIN_GROUP_MIN_VERSION, JoinGroupSubmitError, join_group_options,
    join_group_route,
};

#[test]
fn options_preserve_deadline_lane_and_generated_version_bounds() {
    let deadline = Instant::now();
    let options = join_group_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(JOIN_GROUP_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(JOIN_GROUP_MAX_VERSION));
}

#[test]
fn policy_window_is_an_exact_supported_subset_of_the_wire_descriptor() {
    let supported = JOIN_GROUP_API_DESCRIPTOR.supported_versions;

    assert!(supported.contains(JOIN_GROUP_MIN_VERSION));
    assert!(supported.contains(JOIN_GROUP_MAX_VERSION));
    assert_eq!(JOIN_GROUP_MIN_VERSION.value(), 1);
    assert_eq!(JOIN_GROUP_MAX_VERSION.value(), 3);
}

#[test]
fn coordinator_and_generated_request_group_spellings_must_match() {
    let mut request = JoinGroupRequest::default();
    request.group_id = "group-b".into();

    assert!(matches!(
        join_group_route("group-a", &request),
        Err(JoinGroupSubmitError::GroupMismatch)
    ));
    assert!(join_group_route("group-b", &request).is_ok());
}
