//! Controller route option scenarios for tracked `DeleteTopics` submission.

use std::time::{Duration, Instant};

use kafka_driver::TrafficClass;

use super::delete_topics_submission::delete_topics_options;

#[test]
fn delete_topics_uses_interactive_lane_original_deadline_and_v5_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = delete_topics_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(5))
    );
}
