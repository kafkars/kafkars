//! Linear call wrapper terminal-consumption scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::ListConsumerGroupOffsetsPlan;
use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetsCall},
};

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = ListConsumerGroupOffsetsPlan::new("readers".to_owned(), false)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut call = GroupOffsetsCall::submit(
        &driver,
        plan,
        usize::MAX,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_failure| panic!("accepted call"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    call.recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"))
        .seal();
}
