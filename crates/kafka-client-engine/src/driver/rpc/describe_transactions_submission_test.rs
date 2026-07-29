//! Admin `DescribeTransactions` route, version, and call-ownership scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, CompletionError, CoordinatorKind, Route, TrafficClass};

use crate::{EngineConfig, driver::DriverOwner};

use super::describe_transactions_call::DescribeTransactionsCall;
use super::describe_transactions_submission::{
    describe_transactions_options, describe_transactions_route,
};

#[test]
fn route_targets_the_requested_transaction_coordinator() {
    let Route::Coordinator { key } = describe_transactions_route("orders-writer")
        .unwrap_or_else(|error| panic!("valid route: {error}"))
    else {
        panic!("transaction-coordinator route expected");
    };
    assert_eq!(key.kind(), CoordinatorKind::Transaction);
    assert_eq!(key.as_str(), "orders-writer");
    assert!(describe_transactions_route("").is_err());
}

#[test]
fn options_preserve_original_deadline_interactive_lane_and_exact_v0() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = describe_transactions_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(0)));
}

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = DescribeTransactionsCall::submit(
        &driver,
        "orders-writer",
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
