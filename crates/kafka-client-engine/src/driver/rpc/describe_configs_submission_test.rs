//! Route and option scenarios for tracked generic `DescribeConfigs` submission.

use std::time::{Duration, Instant};

use kafka_client_core::DescribeConfigsRoute;
use kafka_driver::{Route, TrafficClass};

use super::describe_configs_submission::{describe_configs_options, describe_configs_route};

#[test]
fn generic_and_broker_configs_validate_the_core_route_before_submission() {
    assert_eq!(
        describe_configs_route(DescribeConfigsRoute::AnyBroker)
            .unwrap_or_else(|error| panic!("AnyBroker route: {error}")),
        Route::AnyBroker
    );
    assert_eq!(
        describe_configs_route(DescribeConfigsRoute::ExactBroker(7))
            .unwrap_or_else(|error| panic!("exact broker route: {error}")),
        Route::AnyBroker
    );
    assert!(describe_configs_route(DescribeConfigsRoute::ExactBroker(-1)).is_err());
}

#[test]
fn configs_use_interactive_lane_original_deadline_and_v4_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = describe_configs_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(4))
    );
}
