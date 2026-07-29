//! Submission-policy tests for Admin `DescribeTopicPartitions`.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    describe_topic_partitions_call::DescribeTopicPartitionsCall,
    describe_topic_partitions_submission::{
        describe_topic_partitions_options, describe_topic_partitions_route,
    },
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

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeTopicPartitionsCall::submit(
        &driver,
        kafka_wire::DescribeTopicPartitionsRequest::default(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
