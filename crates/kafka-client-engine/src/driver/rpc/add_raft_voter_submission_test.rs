//! Controller route, refresh barrier, deadline, and v0-v1 submission evidence.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{AddRaftVoterEndpoint, AddRaftVoterPlan};
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};
use kafka_wire::AddRaftVoterResponse;

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    AddRaftVoterCall,
    add_raft_voter_submission::{add_raft_voter_options, add_raft_voter_route},
    add_raft_voter_terminal::{
        AddRaftVoterTerminalFact, response_requires_controller_refresh,
        retain_add_raft_voter_terminal,
    },
};

#[test]
fn mutation_uses_controller_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let plan = plan();
    let options = add_raft_voter_options(&plan, deadline);

    assert_eq!(add_raft_voter_route(), Route::Controller);
    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn only_exact_supported_not_controller_responses_request_refresh() {
    for selected_version in 0..=1 {
        assert!(response_requires_controller_refresh(
            Some(selected_version),
            &response(41),
        ));
    }
    assert!(!response_requires_controller_refresh(
        Some(1),
        &response(42)
    ));
    assert!(!response_requires_controller_refresh(Some(1), &response(0)));
    assert!(!response_requires_controller_refresh(None, &response(41)));
    assert!(!response_requires_controller_refresh(
        Some(-1),
        &response(41)
    ));
    assert!(!response_requires_controller_refresh(
        Some(2),
        &response(41)
    ));
}

#[test]
fn no_refresh_terminal_is_ready_without_driver_or_route_evidence() {
    let mut ordinary = terminal(1, 42);
    assert_eq!(ordinary.poll_controller_refresh(None), Some(true));

    let mut missing_route_evidence = terminal(1, 41);
    assert_eq!(
        missing_route_evidence.poll_controller_refresh(None),
        Some(true),
        "a broker code alone cannot forge an invalidation capability"
    );
}

#[test]
fn barrier_retains_known_terminal_and_plan_through_driver_loss_and_completes_once() {
    let expected = plan();
    let mut terminal = terminal_with_plan(1, 41, expected.clone());
    terminal.arm_controller_refresh_for_test();

    for _attempt in 0..2 {
        assert_eq!(terminal.poll_controller_refresh(None), None);
        let AddRaftVoterTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("known broker terminal must survive missing driver ownership");
        };
        assert_eq!(selected_version, Some(1));
        assert_eq!(response.error_code, 41);
        assert_eq!(terminal.plan().voter_id(), expected.voter_id());
        assert_eq!(terminal.plan().listeners()[0].host(), "controller-a");
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
    terminal.discard();
}

#[test]
fn local_write_acknowledgement_raises_the_floor_to_v1() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let plan = plan().with_ack_when_committed(false);
    let options = add_raft_voter_options(&plan, deadline);

    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}

#[test]
fn completion_fault_retains_call_and_exact_voter_plan_after_driver_shutdown() {
    let capture = Arc::new(MonotonicClock::new())
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline: {error}"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let mut call = AddRaftVoterCall::submit(
        &driver,
        &expected,
        capture.operation_deadline(),
        capture.now(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain call and voter plan"));
    assert_eq!(recovered.plan().cluster_id(), expected.cluster_id());
    assert_eq!(recovered.plan().voter_id(), expected.voter_id());
    assert_eq!(
        recovered.plan().voter_directory_id(),
        expected.voter_directory_id()
    );
    assert_eq!(
        recovered.plan().listeners()[0].host(),
        expected.listeners()[0].host()
    );
    recovered.seal();
}

#[test]
fn successful_raw_terminal_retains_exact_voter_plan_until_settlement() {
    let expected = plan();
    let raw = retain_add_raft_voter_terminal(
        Some(ApiVersion::new(1)),
        Ok(AddRaftVoterResponse::default()),
        None,
        expected.clone(),
    );

    assert_eq!(raw.plan().cluster_id(), expected.cluster_id());
    assert_eq!(raw.plan().voter_id(), expected.voter_id());
    assert_eq!(
        raw.plan().voter_directory_id(),
        expected.voter_directory_id()
    );
    assert_eq!(
        raw.plan().listeners()[0].host(),
        expected.listeners()[0].host()
    );
    raw.discard();
}

fn plan() -> AddRaftVoterPlan {
    AddRaftVoterPlan::new(
        Some("cluster-a".to_owned()),
        7,
        [9; 16],
        vec![AddRaftVoterEndpoint::new(
            "CONTROLLER".to_owned(),
            "controller-a".to_owned(),
            9093,
        )],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn terminal(
    selected_version: i16,
    error_code: i16,
) -> super::add_raft_voter_terminal::AddRaftVoterRawTerminal {
    terminal_with_plan(selected_version, error_code, plan())
}

fn terminal_with_plan(
    selected_version: i16,
    error_code: i16,
    plan: AddRaftVoterPlan,
) -> super::add_raft_voter_terminal::AddRaftVoterRawTerminal {
    retain_add_raft_voter_terminal(
        Some(ApiVersion::new(selected_version)),
        response(error_code),
        None,
        plan,
    )
}

fn response(error_code: i16) -> Result<AddRaftVoterResponse, kafka_driver::RequestError> {
    let mut response = AddRaftVoterResponse::default();
    response.error_code = error_code;
    Ok(response)
}
