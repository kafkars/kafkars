//! Admin `DeleteConsumerGroups` coordinator route version-window scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget};
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    DeleteConsumerGroupsCall, DeleteConsumerGroupsRawTerminal,
    delete_consumer_groups_submission::delete_consumer_groups_options,
};

#[test]
fn options_preserve_deadline_lane_and_v0_through_v3_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = delete_consumer_groups_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = DeleteConsumerGroupsTarget::new("workers".to_owned());
    let plan = DeleteConsumerGroupsPlan::new(vec![target.clone()])
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut call = DeleteConsumerGroupsCall::submit(
        &driver,
        plan.clone(),
        target.clone(),
        4_096,
        8_192,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches_evidence(&plan, &target, 4_096, 8_192));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(&plan, &target, 4_096, 8_192));
    recovered.seal();
}

#[test]
fn synchronous_request_rejection_returns_exact_submission_evidence() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let target = DeleteConsumerGroupsTarget::new("workers".to_owned());
    let plan = DeleteConsumerGroupsPlan::new(vec![target.clone()])
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let rejection = DeleteConsumerGroupsCall::submit(
        &driver,
        plan.clone(),
        target.clone(),
        0,
        8_192,
        Instant::now() + Duration::from_secs(1),
    )
    .err()
    .unwrap_or_else(|| panic!("zero request capacity must reject"));
    let (returned_plan, returned_target, request_limit, result_limit) =
        rejection.into_submission_evidence();
    assert_eq!((returned_plan, returned_target), (plan, target));
    assert_eq!((request_limit, result_limit), (0, 8_192));
}

#[test]
fn raw_terminal_rejects_target_and_capacity_mismatches() {
    let target = DeleteConsumerGroupsTarget::new("workers".to_owned());
    let plan = DeleteConsumerGroupsPlan::new(vec![target.clone()])
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let raw = DeleteConsumerGroupsRawTerminal::for_test(plan.clone(), target.clone(), 4_096, 8_192);
    assert!(raw.matches_evidence(&plan, &target, 4_096, 8_192));
    assert!(!raw.matches_evidence(&plan, &target, 4_095, 8_192));
    let other = DeleteConsumerGroupsTarget::new("other".to_owned());
    assert!(!raw.matches_evidence(&plan, &other, 4_096, 8_192));
    raw.discard();
}
