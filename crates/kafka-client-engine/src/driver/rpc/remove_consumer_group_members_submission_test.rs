//! Member-removal coordinator route, lane, version, and deadline scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{ConsumerGroupMemberRemoval, Deadline, RemoveConsumerGroupMembersPlan};
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};
use kafka_wire::LeaveGroupRequest;

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{
    RemoveConsumerGroupMembersCall,
    remove_consumer_group_members_submission::{
        RemoveConsumerGroupMembersSubmitError, remove_consumer_group_members_options,
        remove_consumer_group_members_route,
    },
};

#[test]
fn options_preserve_absolute_deadline_interactive_lane_and_version_floor() {
    let instant = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(7), instant);
    let options = remove_consumer_group_members_options(deadline, 3)
        .unwrap_or_else(|error| panic!("valid options: {error}"));

    assert_eq!(options.deadline(), instant);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version().map(ApiVersion::value), Some(3));
    assert_eq!(options.maximum_version().map(ApiVersion::value), Some(5));
}

#[test]
fn reason_capable_floor_and_invalid_floors_are_explicit() {
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(7), Instant::now());
    let options = remove_consumer_group_members_options(deadline, 5)
        .unwrap_or_else(|error| panic!("valid options: {error}"));
    assert_eq!(options.minimum_version().map(ApiVersion::value), Some(5));
    assert!(matches!(
        remove_consumer_group_members_options(deadline, 2),
        Err(RemoveConsumerGroupMembersSubmitError::InvalidVersionFloor { actual: 2 })
    ));
}

#[test]
fn route_and_generated_request_group_spellings_must_match() {
    let mut request = LeaveGroupRequest::default();
    request.group_id = "group-b".into();

    assert!(matches!(
        remove_consumer_group_members_route("group-a", &request),
        Err(RemoveConsumerGroupMembersSubmitError::GroupMismatch)
    ));
    assert!(remove_consumer_group_members_route("group-b", &request).is_ok());
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = RemoveConsumerGroupMembersPlan::new(
        "workers".to_owned(),
        vec![ConsumerGroupMemberRemoval::new("instance-a".to_owned())],
        None,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let request_scratch_limit = 4_096;
    let result_limit = 8_192;
    let mut call = RemoveConsumerGroupMembersCall::submit(
        &driver,
        plan.clone(),
        request_scratch_limit,
        result_limit,
        deadline,
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.matches(&plan, request_scratch_limit, result_limit));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|_| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches(&plan, request_scratch_limit, result_limit));
    recovered.seal();
}

#[test]
fn synchronous_request_rejection_returns_exact_plan_and_limits() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = RemoveConsumerGroupMembersPlan::new(
        "workers".to_owned(),
        vec![ConsumerGroupMemberRemoval::new("instance-a".to_owned())],
        Some("drain".to_owned()),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let result_limit = 8_192;
    let rejection = RemoveConsumerGroupMembersCall::submit(
        &driver,
        plan.clone(),
        0,
        result_limit,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(10),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .err()
    .unwrap_or_else(|| panic!("zero request envelope must reject"));

    assert_eq!(rejection.into_correlation(), (plan, 0, result_limit));
}
