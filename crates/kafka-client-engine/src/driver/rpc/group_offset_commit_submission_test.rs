//! Route and version-bound scenarios for tracked group `OffsetCommit`.

use std::time::Instant;

use kafka_driver::{CoordinatorKind, Route, TrafficClass};

use super::group_offset_commit_submission::{
    group_offset_commit_options, group_offset_commit_route,
};

#[test]
fn submission_uses_group_coordinator_interactive_lane_and_original_deadline() {
    let deadline = Instant::now();
    let route =
        group_offset_commit_route("readers").unwrap_or_else(|error| panic!("valid route: {error}"));
    let Route::Coordinator { key } = route else {
        panic!("group coordinator route required");
    };
    assert_eq!(key.kind(), CoordinatorKind::Group);
    assert_eq!(key.as_str(), "readers");

    let options = group_offset_commit_options(deadline, false, false, false);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert!(options.rejects_after_route_failure());
    assert_eq!(options.minimum_version(), None);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(9))
    );
}

#[test]
fn leader_epoch_requires_v6_without_changing_v9_ceiling() {
    let options = group_offset_commit_options(Instant::now(), true, false, false);
    assert_eq!(
        options.minimum_version(),
        Some(kafka_driver::ApiVersion::new(6))
    );
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(9))
    );
}

#[test]
fn static_membership_requires_v7_without_changing_v9_ceiling() {
    let options = group_offset_commit_options(Instant::now(), false, true, false);
    assert_eq!(
        options.minimum_version(),
        Some(kafka_driver::ApiVersion::new(7))
    );
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(9))
    );
}

#[test]
fn consumer_group_protocol_requires_exactly_v9() {
    let options = group_offset_commit_options(Instant::now(), false, false, true);
    assert_eq!(
        options.minimum_version(),
        Some(kafka_driver::ApiVersion::new(9))
    );
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(9))
    );
}

#[test]
fn empty_group_is_rejected_before_driver_ownership() {
    assert!(group_offset_commit_route("").is_err());
}
