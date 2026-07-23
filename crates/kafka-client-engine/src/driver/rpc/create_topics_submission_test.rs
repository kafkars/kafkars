//! Controller route option scenarios for tracked `CreateTopics` submission.

use std::time::{Duration, Instant};

use kafka_driver::TrafficClass;

use super::create_topics_submission::create_topics_options;

#[test]
fn create_topics_uses_interactive_lane_original_deadline_and_v7_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = create_topics_options(deadline);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options.maximum_version(),
        Some(kafka_driver::ApiVersion::new(7))
    );
}
