//! Any-broker option scenarios for transient `DescribeTopics` submission.

use std::time::{Duration, Instant};

use kafka_driver::{Route, TrafficClass};
use kafka_wire::METADATA_API_DESCRIPTOR;

use super::describe_topics_submission::{
    DESCRIBE_TOPICS_AUTHORIZED_OPERATIONS_MIN_VERSION, DESCRIBE_TOPICS_ID_MIN_VERSION,
    DESCRIBE_TOPICS_MAX_VERSION, DESCRIBE_TOPICS_MIN_VERSION, describe_topics_options,
    describe_topics_route,
};

#[test]
fn describe_topics_uses_interactive_lane_deadline_and_exact_version_window() {
    let deadline = Instant::now() + Duration::from_secs(9);
    assert_eq!(describe_topics_route(), Route::AnyBroker);
    let options = describe_topics_options(deadline, DESCRIBE_TOPICS_MIN_VERSION);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(DESCRIBE_TOPICS_MIN_VERSION));
    assert_eq!(options.maximum_version(), Some(DESCRIBE_TOPICS_MAX_VERSION));
}

#[test]
fn policy_window_is_an_exact_supported_subset_of_the_wire_descriptor() {
    let supported = METADATA_API_DESCRIPTOR.supported_versions;

    assert!(supported.contains(DESCRIBE_TOPICS_MIN_VERSION));
    assert!(supported.contains(DESCRIBE_TOPICS_AUTHORIZED_OPERATIONS_MIN_VERSION));
    assert!(supported.contains(DESCRIBE_TOPICS_ID_MIN_VERSION));
    assert!(supported.contains(DESCRIBE_TOPICS_MAX_VERSION));
    assert_eq!(DESCRIBE_TOPICS_MIN_VERSION.value(), 4);
    assert_eq!(DESCRIBE_TOPICS_AUTHORIZED_OPERATIONS_MIN_VERSION.value(), 8);
    assert_eq!(DESCRIBE_TOPICS_ID_MIN_VERSION.value(), 12);
    assert_eq!(DESCRIBE_TOPICS_MAX_VERSION.value(), 13);
}

#[test]
fn topic_id_window_requires_nullable_missing_topic_names() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = describe_topics_options(deadline, DESCRIBE_TOPICS_ID_MIN_VERSION);
    assert_eq!(
        options.minimum_version(),
        Some(DESCRIBE_TOPICS_ID_MIN_VERSION)
    );
    assert_eq!(options.maximum_version(), Some(DESCRIBE_TOPICS_MAX_VERSION));
}
