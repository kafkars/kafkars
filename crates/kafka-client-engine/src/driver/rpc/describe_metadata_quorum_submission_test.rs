//! Submission-policy tests for Admin `DescribeMetadataQuorum`.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::describe_metadata_quorum_submission::{
    describe_metadata_quorum_options, describe_metadata_quorum_route,
};

#[test]
fn request_is_fixed_to_any_broker_interactive_versions_zero_through_two() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let options = describe_metadata_quorum_options(deadline);

    assert_eq!(describe_metadata_quorum_route(), Route::AnyBroker);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}
