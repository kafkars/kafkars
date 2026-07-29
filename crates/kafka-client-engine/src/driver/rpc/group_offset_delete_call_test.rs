//! Linear call-wrapper completion and shutdown-recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsPlan};
use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetDeleteCall},
};

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = DeleteConsumerGroupOffsetsPlan::new(
        "readers".to_owned(),
        vec![DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 0)],
    )
    .unwrap_or_else(|error| panic!("valid deletion plan: {error}"));
    let mut call = GroupOffsetDeleteCall::submit(
        &driver,
        plan,
        usize::MAX,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|failure| panic!("accepted call: {failure}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted ownership"));
    recovered.seal();
}
