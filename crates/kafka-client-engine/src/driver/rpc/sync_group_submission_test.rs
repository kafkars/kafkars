//! `SyncGroup` route and absolute-deadline submission scenarios.

use std::time::Instant;

use kafka_driver::TrafficClass;
use kafka_wire::{SYNC_GROUP_API_DESCRIPTOR, SyncGroupRequest};

use super::sync_group_submission::{
    STATIC_SYNC_GROUP_VERSION, SYNC_GROUP_MAX_VERSION, SYNC_GROUP_MIN_VERSION,
    SyncGroupSubmitError, sync_group_options, sync_group_route,
};

#[test]
fn options_preserve_deadline_lane_and_generated_version_bounds() {
    let deadline = Instant::now();
    let options = sync_group_options(deadline, false);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(SYNC_GROUP_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(SYNC_GROUP_MAX_VERSION));
}

#[test]
fn static_membership_uses_the_exact_v3_window() {
    let options = sync_group_options(Instant::now(), true);
    assert_eq!(options.minimum_version(), Some(STATIC_SYNC_GROUP_VERSION));
    assert_eq!(options.maximum_version(), Some(STATIC_SYNC_GROUP_VERSION));
}

#[test]
fn policy_window_is_an_exact_supported_subset_of_the_wire_descriptor() {
    let supported = SYNC_GROUP_API_DESCRIPTOR.supported_versions;

    assert!(supported.contains(SYNC_GROUP_MIN_VERSION));
    assert!(supported.contains(SYNC_GROUP_MAX_VERSION));
    assert_eq!(SYNC_GROUP_MIN_VERSION.value(), 0);
    assert_eq!(SYNC_GROUP_MAX_VERSION.value(), 2);
}

#[test]
fn coordinator_and_generated_request_group_spellings_must_match() {
    let mut request = SyncGroupRequest::default();
    request.group_id = "group-b".into();

    assert!(matches!(
        sync_group_route("group-a", &request),
        Err(SyncGroupSubmitError::GroupMismatch)
    ));
    assert!(sync_group_route("group-b", &request).is_ok());
}
