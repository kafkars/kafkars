//! Exact-broker route and version-window evidence for `DescribeReplicaLogDirs`.

use std::time::{Duration, Instant};

use kafka_client_core::DescribeReplicaLogDirsReplica;
use kafka_driver::{CompletionError, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DescribeReplicaLogDirsCall,
    describe_replica_log_dirs_submission::{
        describe_replica_log_dirs_options, describe_replica_log_dirs_route,
    },
};

#[test]
fn route_targets_the_requested_broker() {
    assert_eq!(
        describe_replica_log_dirs_route(17)
            .unwrap_or_else(|error| panic!("valid broker: {error:?}")),
        Route::AnyBroker
    );
    assert!(describe_replica_log_dirs_route(-1).is_err());
}

#[test]
fn options_preserve_deadline_lane_and_supported_versions() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let options = describe_replica_log_dirs_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(
        options
            .minimum_version()
            .map(kafka_wire_core::ApiVersion::value),
        Some(1)
    );
    assert_eq!(
        options
            .maximum_version()
            .map(kafka_wire_core::ApiVersion::value),
        Some(5)
    );
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeReplicaLogDirsCall::submit(
        &driver,
        1,
        &[DescribeReplicaLogDirsReplica::new(
            "orders".to_owned(),
            0,
            1,
        )],
        4_096,
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
