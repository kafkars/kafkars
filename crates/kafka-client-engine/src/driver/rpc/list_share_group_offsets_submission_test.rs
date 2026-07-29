//! API-90 group route, exact version window, traffic, and deadline scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};
use kafka_wire::{
    DescribeShareGroupOffsetsRequest,
    describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestGroup,
};

use super::list_share_group_offsets_submission::{
    ListShareGroupOffsetsSubmitError, list_share_group_offsets_options,
    list_share_group_offsets_route,
};

#[test]
fn submission_uses_group_coordinator_interactive_original_deadline_and_v0_v1() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = list_share_group_offsets_options(deadline);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);

    let request = request("share-readers");
    let route = list_share_group_offsets_route("share-readers", &request)
        .unwrap_or_else(|error| panic!("valid route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("API 90 must use group-coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "share-readers");
}

#[test]
fn route_rejects_group_count_and_identity_drift_before_driver_ownership() {
    assert!(matches!(
        list_share_group_offsets_route(
            "share-readers",
            &DescribeShareGroupOffsetsRequest::default(),
        ),
        Err(ListShareGroupOffsetsSubmitError::GroupCount { actual: 0 })
    ));
    assert!(matches!(
        list_share_group_offsets_route("share-readers", &request("other")),
        Err(ListShareGroupOffsetsSubmitError::GroupMismatch)
    ));
}

fn request(group_id: &str) -> DescribeShareGroupOffsetsRequest {
    let mut group = DescribeShareGroupOffsetsRequestGroup::default();
    group.group_id = group_id.into();
    let mut request = DescribeShareGroupOffsetsRequest::default();
    request.groups = vec![group];
    request
}
