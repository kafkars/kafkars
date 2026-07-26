//! Linear call wrapper terminal-consumption scenarios.

use std::time::{Duration, Instant};

use kafka_driver::CompletionError;

use crate::{EngineConfig, driver::DriverOwner};

use super::GroupOffsetsCall;

#[test]
fn completion_fault_is_yielded_once_and_is_not_recovered_as_active() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = GroupOffsetsCall::submit(
        &driver,
        "readers",
        false,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|failure| panic!("accepted call: {}", failure.into_source()));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.try_terminal().is_none());
    assert!(call.recover_after_driver_shutdown().is_none());
}
