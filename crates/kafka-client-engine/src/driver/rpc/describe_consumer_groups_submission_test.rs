//! Coordinator route, deadline, traffic, and version-window tests.

use std::time::{Duration, Instant};

use kafka_client_core::{AdminDescribeConsumerGroupsCallKind, Deadline};
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{
    DescribeConsumerGroupsCall,
    describe_consumer_groups_submission::describe_consumer_groups_options,
};

#[test]
fn authorization_intent_raises_only_the_exact_version_floor() {
    let deadline = Instant::now() + Duration::from_secs(3);
    let ordinary = describe_consumer_groups_options(deadline, false);
    assert_eq!(ordinary.deadline(), deadline);
    assert_eq!(ordinary.traffic_class(), TrafficClass::Interactive);
    assert_eq!(ordinary.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(ordinary.maximum_version(), Some(ApiVersion::new(6)));

    let authorized = describe_consumer_groups_options(deadline, true);
    assert_eq!(authorized.minimum_version(), Some(ApiVersion::new(3)));
    assert_eq!(authorized.maximum_version(), Some(ApiVersion::new(6)));
}

#[test]
fn classic_and_modern_completion_faults_remain_recoverable_after_shutdown() {
    for call_kind in [
        AdminDescribeConsumerGroupsCallKind::ClassicFallback,
        AdminDescribeConsumerGroupsCallKind::Consumer,
    ] {
        let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
            .unwrap_or_else(|error| panic!("driver owner: {error}"));
        let deadline = OperationDeadline::from_parts_for_test(
            Deadline::from_tick(10),
            Instant::now() + Duration::from_secs(1),
        );
        let mut call = DescribeConsumerGroupsCall::submit(
            &driver,
            call_kind,
            "workers".to_owned(),
            false,
            4_096,
            8_192,
            deadline,
        )
        .unwrap_or_else(|_error| panic!("accepted call"));
        drop(driver);

        assert!(matches!(
            call.try_terminal(),
            Some(Err(CompletionError::Closed))
        ));
        let recovered = call
            .recover_after_driver_shutdown()
            .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
        assert!(recovered.matches_evidence("workers", false, call_kind, 4_096, 8_192));
        recovered.seal();
    }
}

#[test]
fn request_rejection_returns_exact_route_intent_and_bounds() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let rejection = match DescribeConsumerGroupsCall::submit(
        &driver,
        AdminDescribeConsumerGroupsCallKind::Consumer,
        "workers".to_owned(),
        true,
        0,
        9_001,
        deadline,
    ) {
        Ok(_call) => panic!("zero request capacity must reject"),
        Err(rejection) => rejection,
    };
    let (group_id, authorized, call_kind, request_limit, result_limit) = rejection.into_evidence();
    assert_eq!(group_id, "workers");
    assert!(authorized);
    assert_eq!(call_kind, AdminDescribeConsumerGroupsCallKind::Consumer);
    assert_eq!(request_limit, 0);
    assert_eq!(result_limit, 9_001);
}
