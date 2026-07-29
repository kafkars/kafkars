//! Controller route, refresh barrier, deadline, and exact-v0 submission evidence.

use std::time::{Duration, Instant};

use kafka_client_core::UnregisterBrokerPlan;
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};
use kafka_wire::UnregisterBrokerResponse;

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    UnregisterBrokerCall,
    unregister_broker_submission::{unregister_broker_options, unregister_broker_route},
    unregister_broker_terminal::{
        UnregisterBrokerTerminalFact, response_requires_controller_refresh,
        retain_unregister_broker_terminal,
    },
};

#[test]
fn mutation_uses_controller_and_preserves_original_deadline() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let options = unregister_broker_options(deadline);

    assert_eq!(unregister_broker_route(), Route::Controller);
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
fn barrier_retains_known_terminal_through_driver_loss_and_completes_once() {
    let mut terminal = terminal(41);
    terminal.arm_controller_refresh_for_test();

    for _attempt in 0..2 {
        assert_eq!(terminal.poll_controller_refresh(None), None);
        let UnregisterBrokerTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("known broker terminal must survive missing driver ownership");
        };
        assert_eq!(selected_version, Some(0));
        assert_eq!(response.error_code, 41);
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
fn completion_fault_retains_call_and_broker_correlation_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call =
        UnregisterBrokerCall::submit(&driver, plan(7), Instant::now() + Duration::from_secs(1))
            .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain call and broker correlation"));
    assert_eq!(recovered.broker_id_for_test(), 7);
    recovered.seal();
}

#[test]
fn successful_raw_terminal_retains_broker_correlation_until_settlement() {
    let raw = retain_unregister_broker_terminal(
        Some(ApiVersion::new(0)),
        Ok(UnregisterBrokerResponse::default()),
        None,
        plan(7),
    );

    assert_eq!(raw.broker_id_for_test(), 7);
    raw.discard();
}

fn plan(broker_id: i32) -> UnregisterBrokerPlan {
    UnregisterBrokerPlan::new(broker_id).unwrap_or_else(|error| panic!("plan: {error}"))
}

fn terminal(error_code: i16) -> super::unregister_broker_terminal::UnregisterBrokerRawTerminal {
    retain_unregister_broker_terminal(
        Some(ApiVersion::new(0)),
        response(error_code),
        None,
        plan(7),
    )
}

fn response(error_code: i16) -> Result<UnregisterBrokerResponse, kafka_driver::RequestError> {
    let mut response = UnregisterBrokerResponse::default();
    response.error_code = error_code;
    Ok(response)
}
