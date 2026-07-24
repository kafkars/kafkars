//! Route and option scenarios for tracked topic `DescribeConfigs` submission.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::describe_configs_submission::{describe_configs_options, describe_configs_route};

#[test]
fn topic_configs_use_any_broker_interactive_lane_original_deadline_and_v4_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = describe_configs_options(deadline);
    assert_eq!(describe_configs_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(4))
    );
}
