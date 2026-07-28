//! Exact call ownership and recovery scenarios for reassignment listing.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, ListPartitionReassignmentsPlan, Moment};
use kafka_driver::CompletionError;

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::ListPartitionReassignmentsCall;

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ListPartitionReassignmentsPlan::all_active();
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let result_limit = 4096;
    let mut call = ListPartitionReassignmentsCall::submit(
        &driver,
        plan.clone(),
        result_limit,
        Moment::from_tick(1),
        deadline,
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.matches(&plan, result_limit));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|_| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches(&plan, result_limit));
    recovered.seal();
}

#[test]
fn synchronous_deadline_rejection_returns_exact_plan_and_result_limit() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ListPartitionReassignmentsPlan::all_active();
    let result_limit = 8192;
    let rejection = ListPartitionReassignmentsCall::submit(
        &driver,
        plan.clone(),
        result_limit,
        Moment::from_tick(10),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(10),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .err()
    .unwrap_or_else(|| panic!("elapsed core deadline must reject"));

    assert_eq!(rejection.into_correlation(), (plan, result_limit));
}
