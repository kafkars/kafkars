//! Singleton coordinator route, exact v1, traffic, and deadline scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};
use kafka_wire::ShareGroupDescribeRequest;

use super::describe_share_group_submission::{
    DescribeShareGroupSubmitError, describe_share_group_options, describe_share_group_route,
};

#[test]
fn submission_uses_group_coordinator_interactive_original_deadline_and_exact_v1() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = describe_share_group_options(deadline);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);

    let mut request = ShareGroupDescribeRequest::default();
    request.group_ids = vec!["share-readers".into()];
    let route = describe_share_group_route("share-readers", &request)
        .unwrap_or_else(|error| panic!("valid route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("API 77 must use group-coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "share-readers");
}

#[test]
fn route_rejects_non_singleton_and_group_drift_before_driver_ownership() {
    let request = ShareGroupDescribeRequest::default();
    assert!(matches!(
        describe_share_group_route("share-readers", &request),
        Err(DescribeShareGroupSubmitError::InvalidGroupBatch)
    ));

    let mut request = ShareGroupDescribeRequest::default();
    request.group_ids = vec!["other".into()];
    assert!(matches!(
        describe_share_group_route("share-readers", &request),
        Err(DescribeShareGroupSubmitError::GroupMismatch)
    ));
}
