//! Linear API-90 call completion and post-driver recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::ListShareGroupOffsetsPlan;
use kafka_driver::CompletionError;

use crate::{EngineConfig, driver::DriverOwner};

use super::ListShareGroupOffsetsCall;

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ListShareGroupOffsetsPlan::all("share-readers".to_owned())
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut call =
        ListShareGroupOffsetsCall::submit(&driver, &plan, Instant::now() + Duration::from_secs(1))
            .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
