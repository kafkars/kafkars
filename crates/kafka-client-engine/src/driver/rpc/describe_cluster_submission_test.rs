//! Any-broker option scenarios for `DescribeCluster` submission.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::describe_cluster_submission::{describe_cluster_options, describe_cluster_route};

#[test]
fn describe_cluster_uses_interactive_lane_original_deadline_and_v2_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    assert_eq!(describe_cluster_route(), Route::AnyBroker);
    let options = describe_cluster_options(deadline, false, false);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(2))
    );
    assert_eq!(
        describe_cluster_options(deadline, true, false).maximum_version(),
        Some(kafka_driver::ApiVersion::new(2))
    );
    assert_eq!(
        describe_cluster_options(deadline, false, true).maximum_version(),
        Some(kafka_driver::ApiVersion::new(2))
    );
    assert_eq!(
        describe_cluster_options(deadline, true, true).maximum_version(),
        Some(kafka_driver::ApiVersion::new(2))
    );
}
