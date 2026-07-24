//! Any-broker option scenarios for transient `DescribeTopics` submission.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};

use super::describe_topics_submission::{describe_topics_options, describe_topics_route};

#[test]
fn describe_topics_uses_interactive_lane_original_deadline_and_v13_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    assert_eq!(describe_topics_route(), Route::AnyBroker);
    let options = describe_topics_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(13))
    );
}
