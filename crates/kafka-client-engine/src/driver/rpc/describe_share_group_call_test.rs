//! Linear call completion and post-driver recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::DescribeShareGroupPlan;
use kafka_driver::CompletionError;

use crate::{EngineConfig, driver::DriverOwner};

use super::DescribeShareGroupCall;

#[test]
fn completion_fault_is_yielded_once_and_not_recovered_as_active() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = DescribeShareGroupPlan::new("share-readers".to_owned(), false)
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut call =
        DescribeShareGroupCall::submit(&driver, &plan, Instant::now() + Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.try_terminal().is_none());
    assert!(call.recover_after_driver_shutdown().is_none());
}
