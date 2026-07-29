//! Exact-broker route and version-window evidence for `DescribeReplicaLogDirs`.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::describe_replica_log_dirs_submission::{
    describe_replica_log_dirs_options, describe_replica_log_dirs_route,
};

#[test]
fn route_targets_the_requested_broker() {
    assert_eq!(
        describe_replica_log_dirs_route(17).expect("valid broker"),
        Route::AnyBroker
    );
    assert!(describe_replica_log_dirs_route(-1).is_err());
}

#[test]
fn options_preserve_deadline_lane_and_supported_versions() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let options = describe_replica_log_dirs_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.minimum_version().map(|version| version.value()),
        Some(1)
    );
    assert_eq!(
        options.maximum_version().map(|version| version.value()),
        Some(5)
    );
}
