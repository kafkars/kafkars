//! Version and traffic policy tests for API27 transaction aborts.

use std::time::{Duration, Instant};

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_driver::{ApiVersion, CompletionError, TrafficClass};
use kafka_wire::WriteTxnMarkersResponse;

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    AbortPartitionTransactionCall,
    abort_partition_transaction_submission::abort_partition_transaction_options,
    abort_partition_transaction_terminal::retain_abort_partition_transaction_terminal,
};

#[test]
fn default_transaction_version_keeps_exact_v1_v2_interactive_window() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let plan = plan();
    let options = abort_partition_transaction_options(&plan, deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn transaction_version_two_raises_the_floor_to_v2() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let plan = plan()
        .with_transaction_version(2)
        .expect("valid transaction version");
    let options = abort_partition_transaction_options(&plan, deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(2)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn completion_fault_retains_call_and_exact_transaction_plan_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let mut call = AbortPartitionTransactionCall::submit(
        &driver,
        expected.clone(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain call and transaction plan"));
    assert!(recovered.matches_plan_for_test(&expected));
    recovered.seal();
}

#[test]
fn successful_raw_terminal_retains_exact_transaction_plan_until_settlement() {
    let expected = plan();
    let raw = retain_abort_partition_transaction_terminal(
        Some(ApiVersion::new(2)),
        Ok(WriteTxnMarkersResponse::default()),
        None,
        expected.clone(),
    );

    assert!(raw.matches_plan_for_test(&expected));
    raw.discard();
}

fn plan() -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11).expect("valid abort plan")
}
