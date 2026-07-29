//! Coordinator route, exact version, traffic, group, and deadline scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CoordinatorKind, Route, TrafficClass};
use kafka_wire::DeleteShareGroupOffsetsRequest;

use super::delete_share_group_offsets_submission::{
    DeleteShareGroupOffsetsSubmitError, delete_share_group_offsets_options,
    delete_share_group_offsets_route,
};

#[test]
fn submission_uses_group_coordinator_interactive_original_deadline_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = delete_share_group_offsets_options(deadline);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.deadline(), deadline);

    let mut request = DeleteShareGroupOffsetsRequest::default();
    request.group_id = "share-readers".into();
    let route = delete_share_group_offsets_route("share-readers", &request)
        .unwrap_or_else(|error| panic!("valid route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("API 92 must use group-coordinator routing");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "share-readers");
}

#[test]
fn route_rejects_group_drift_before_driver_ownership() {
    let mut request = DeleteShareGroupOffsetsRequest::default();
    request.group_id = "other".into();
    assert!(matches!(
        delete_share_group_offsets_route("share-readers", &request),
        Err(DeleteShareGroupOffsetsSubmitError::GroupMismatch)
    ));
}
