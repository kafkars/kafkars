//! Submission-policy tests for Admin `DescribeTopicPartitions`.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_topic_partitions_submission::{
    describe_topic_partitions_options, describe_topic_partitions_route,
};

#[test]
fn request_uses_any_broker_and_preserves_the_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_topic_partitions_options(deadline);

    assert_eq!(describe_topic_partitions_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
}

#[test]
fn request_is_pinned_to_exactly_version_zero() {
    let options = describe_topic_partitions_options(Instant::now());

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}
