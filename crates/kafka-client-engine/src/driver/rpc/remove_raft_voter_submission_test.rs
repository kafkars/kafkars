//! Controller route, refresh barrier, deadline, and exact-v0 submission evidence.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};
use kafka_wire::RemoveRaftVoterResponse;

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    RemoveRaftVoterCall,
    remove_raft_voter_submission::{remove_raft_voter_options, remove_raft_voter_route},
    remove_raft_voter_terminal::{
        RemoveRaftVoterTerminalFact, response_requires_controller_refresh,
        retain_remove_raft_voter_terminal,
    },
};

#[test]
fn mutation_uses_controller_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = remove_raft_voter_options(deadline);

    assert_eq!(remove_raft_voter_route(), Route::Controller);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn only_exact_v0_not_controller_response_requests_refresh() {
    assert!(response_requires_controller_refresh(Some(0), &response(41)));
    assert!(!response_requires_controller_refresh(
        Some(0),
        &response(42)
    ));
    assert!(!response_requires_controller_refresh(Some(0), &response(0)));
    assert!(!response_requires_controller_refresh(None, &response(41)));
    assert!(!response_requires_controller_refresh(
        Some(-1),
        &response(41)
    ));
    assert!(!response_requires_controller_refresh(
        Some(1),
        &response(41)
    ));
}

#[test]
fn no_refresh_terminal_is_ready_without_driver_or_route_evidence() {
    let mut ordinary = terminal(42);
    assert_eq!(ordinary.poll_controller_refresh(None), Some(true));

    let mut missing_route_evidence = terminal(41);
    assert_eq!(
        missing_route_evidence.poll_controller_refresh(None),
        Some(true),
        "a broker code alone cannot forge an invalidation capability"
    );
}

#[test]
fn barrier_retains_known_terminal_and_plan_through_driver_loss_and_completes_once() {
    let expected = plan();
    let mut terminal = terminal(41);
    terminal.arm_controller_refresh_for_test();

    for _attempt in 0..2 {
        assert_eq!(terminal.poll_controller_refresh(None), None);
        let RemoveRaftVoterTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("known broker terminal must survive missing driver ownership");
        };
        assert_eq!(selected_version, Some(0));
        assert_eq!(response.error_code, 41);
        assert!(terminal.matches_plan_for_test(&expected));
    }

    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    assert_eq!(terminal.poll_controller_refresh(Some(&driver)), Some(false));
    assert_eq!(terminal.poll_controller_refresh(Some(&driver)), Some(false));
    assert_eq!(terminal.poll_controller_refresh(Some(&driver)), Some(true));
    assert_eq!(
        terminal.poll_controller_refresh(Some(&driver)),
        Some(true),
        "completed refresh authority cannot submit a second invalidation"
    );
    assert!(terminal.matches_plan_for_test(&expected));
    terminal.discard();
}

#[test]
fn completion_fault_retains_call_and_exact_voter_plan_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let mut call = RemoveRaftVoterCall::submit(&driver, expected.clone(), deadline())
        .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain call and voter plan"));
    assert!(recovered.matches_plan_for_test(&expected));
    recovered.seal();
}

fn plan() -> kafka_client_core::RemoveRaftVoterPlan {
    kafka_client_core::RemoveRaftVoterPlan::new(Some("cluster-a".to_owned()), 7, [9; 16])
        .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn deadline() -> crate::clock::OperationDeadline {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
        .operation_deadline()
}

fn terminal(error_code: i16) -> super::remove_raft_voter_terminal::RemoveRaftVoterRawTerminal {
    retain_remove_raft_voter_terminal(Some(ApiVersion::new(0)), response(error_code), None, plan())
}

fn response(error_code: i16) -> Result<RemoveRaftVoterResponse, kafka_driver::RequestError> {
    let mut response = RemoveRaftVoterResponse::default();
    response.error_code = error_code;
    Ok(response)
}
