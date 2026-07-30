//! Transaction initialization call ownership and shutdown scenarios.

use std::time::{Duration, Instant};

use kafka_driver::CompletionError;

use crate::EngineConfig;

use super::{super::DriverOwner, TransactionInitCall, TransactionInitPoll};

#[test]
fn call_owner_is_linear_at_the_adapter_boundary() {
    fn consume(_call: TransactionInitCall) {}
    let _: fn(TransactionInitCall) = consume;
}

#[test]
fn accepted_call_yields_one_closed_completion_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = TransactionInitCall::submit(
        &driver,
        "writer",
        30_000,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.poll(),
        TransactionInitPoll::Terminal(Err(CompletionError::Closed))
    ));
    assert!(matches!(call.poll(), TransactionInitPoll::Pending));
    assert!(call.recover_after_driver_shutdown().is_none());
}

#[test]
fn refresh_state_reports_progress_then_recovers_the_known_terminal() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = TransactionInitCall::refreshing_for_test(&driver, 16);

    assert!(matches!(call.poll(), TransactionInitPoll::Progress));
    assert!(matches!(call.poll(), TransactionInitPoll::Pending));
    let terminal = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("refreshing call retains its known terminal"));
    let super::super::TransactionInitTerminalFact::Response { response, .. } = terminal.fact()
    else {
        panic!("broker terminal retained through refresh recovery");
    };
    assert_eq!(response.error_code, 16);
    terminal.discard();
}
