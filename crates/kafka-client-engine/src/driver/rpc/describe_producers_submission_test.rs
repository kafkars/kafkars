//! Admin `DescribeProducers` partition-leader route and v0 policy scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, PartitionId, Route, TopicName, TrafficClass};

use super::describe_producers_submission::{describe_producers_options, describe_producers_route};

#[test]
fn route_targets_the_requested_partition_leader() {
    assert_eq!(
        describe_producers_route("orders", 17, None)
            .unwrap_or_else(|error| panic!("valid route: {error}")),
        Route::PartitionLeader {
            topic: TopicName::new("orders".to_owned())
                .unwrap_or_else(|error| panic!("valid topic: {error}")),
            partition: PartitionId::new(17)
                .unwrap_or_else(|error| panic!("valid partition: {error}")),
        }
    );
    assert!(describe_producers_route("", 17, None).is_err());
    assert!(describe_producers_route("orders", -1, None).is_err());
}

#[test]
fn explicit_broker_selects_exact_route_after_validating_the_target() {
    assert_eq!(
        describe_producers_route("orders", 17, Some(7))
            .unwrap_or_else(|error| panic!("valid exact broker route: {error}")),
        Route::AnyBroker
    );
    assert!(describe_producers_route("", 17, Some(7)).is_err());
    assert!(describe_producers_route("orders", -1, Some(7)).is_err());
    assert!(describe_producers_route("orders", 17, Some(-1)).is_err());
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_producers_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
