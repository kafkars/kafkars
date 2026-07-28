//! Accepted-call ownership and exact synchronous-rejection evidence.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AlterPartitionReassignment, AlterPartitionReassignmentsPlan, Deadline, Moment,
    PartitionReassignmentTarget,
};
use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{AlterPartitionReassignmentsCall, DriverOwner},
};

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
        "orders".to_owned(),
        0,
        PartitionReassignmentTarget::Replicas(vec![1, 2]),
    )])
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    );
    let expected_plan = plan.clone();
    let mut call = AlterPartitionReassignmentsCall::submit(
        &driver,
        plan,
        4_096,
        8_192,
        deadline,
        Moment::from_tick(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    assert!(call.matches_evidence(&expected_plan, 4_096, 8_192));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(&expected_plan, 4_096, 8_192));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_exact_submission_evidence() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = AlterPartitionReassignmentsPlan::new(vec![AlterPartitionReassignment::new(
        "orders".to_owned(),
        0,
        PartitionReassignmentTarget::Cancel,
    )])
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let expected_plan = plan.clone();
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(1),
        Instant::now() + Duration::from_secs(1),
    );
    let rejection = match AlterPartitionReassignmentsCall::submit(
        &driver,
        plan,
        4_096,
        8_192,
        deadline,
        Moment::from_tick(1),
    ) {
        Ok(_call) => panic!("elapsed deadline must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(
        rejection.into_submission_evidence(),
        (expected_plan, 4_096, 8_192)
    );
}
