//! Submission-policy tests for Admin `DescribeMetadataQuorum`.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    describe_metadata_quorum_call::DescribeMetadataQuorumCall,
    describe_metadata_quorum_submission::{
        describe_metadata_quorum_options, describe_metadata_quorum_route,
    },
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

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call =
        DescribeMetadataQuorumCall::submit(&driver, Instant::now() + Duration::from_secs(1))
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
