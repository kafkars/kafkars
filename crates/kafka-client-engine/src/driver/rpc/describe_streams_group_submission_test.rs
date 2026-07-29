//! Coordinator route, conditional version floor, traffic, and deadline scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};
use kafka_wire::StreamsGroupDescribeRequest;

use super::describe_streams_group_submission::{
    DescribeStreamsGroupSubmitError, describe_streams_group_options, describe_streams_group_route,
};

#[test]
fn submission_uses_group_coordinator_interactive_original_deadline_and_stable_versions() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = describe_streams_group_options(deadline, false);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);

    let topology_options = describe_streams_group_options(deadline, true);
    assert_eq!(topology_options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(topology_options.maximum_version(), Some(ApiVersion::new(1)));

    let mut request = StreamsGroupDescribeRequest::default();
    request.group_ids = vec!["streams-app".into()];
    let route = describe_streams_group_route("streams-app", &request)
        .unwrap_or_else(|error| panic!("valid route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("API 89 must use group-coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "streams-app");
}

#[test]
fn route_rejects_non_singleton_and_group_drift_before_driver_ownership() {
    let request = StreamsGroupDescribeRequest::default();
    assert!(matches!(
        describe_streams_group_route("streams-app", &request),
        Err(DescribeStreamsGroupSubmitError::InvalidGroupBatch)
    ));

    let mut request = StreamsGroupDescribeRequest::default();
    request.group_ids = vec!["other".into()];
    assert!(matches!(
        describe_streams_group_route("streams-app", &request),
        Err(DescribeStreamsGroupSubmitError::GroupMismatch)
    ));
}
