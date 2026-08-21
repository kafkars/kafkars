//! Admin `DescribeProducers` route, version, and call-ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::AdminDescribeProducerTarget;
use kafka_driver::{
    ApiVersion, BrokerId, CompletionError, PartitionId, Route, TopicName, TrafficClass,
};

use crate::{EngineConfig, driver::DriverOwner};

use super::describe_producers_call::DescribeProducersCall;
use super::describe_producers_submission::{describe_producers_options, describe_producers_route};

#[test]
fn route_targets_the_requested_partition_leader() {
    assert_eq!(
        describe_producers_route("orders", 17, None)
            .unwrap_or_else(|error| panic!("valid route: {error}")),
        Route::PartitionLeader {
            topic: TopicName::new("orders".to_owned())
                .unwrap_or_else(|error| panic!("valid topic: {error}")),
            partition: PartitionId::new(17)
                .unwrap_or_else(|error| panic!("valid partition: {error}")),
        }
    );
    assert!(describe_producers_route("", 17, None).is_err());
    assert!(describe_producers_route("orders", -1, None).is_err());
}

#[test]
fn explicit_broker_selects_exact_route_after_validating_the_target() {
    assert_eq!(
        describe_producers_route("orders", 17, Some(7))
            .unwrap_or_else(|error| panic!("valid exact broker route: {error}")),
        Route::Broker {
            broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker: {error}")),
        }
    );
    assert!(describe_producers_route("", 17, Some(7)).is_err());
    assert!(describe_producers_route("orders", -1, Some(7)).is_err());
    assert!(describe_producers_route("orders", 17, Some(-1)).is_err());
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_producers_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = AdminDescribeProducerTarget::new("orders".to_owned(), 2);
    let mut call = DescribeProducersCall::submit(
        &driver,
        &target,
        Some(7),
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
