//! Linear call-wrapper completion and shutdown-recovery scenarios.

use std::time::{Duration, Instant};

use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetDeleteCall},
    protocol::admin::group_offset_delete::OffsetDeleteTargetRef,
};

#[test]
fn completion_fault_is_yielded_once_and_not_recovered_as_active() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = GroupOffsetDeleteCall::submit(
        &driver,
        "readers",
        &[OffsetDeleteTargetRef::new("orders", 0)],
        usize::MAX,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|failure| panic!("accepted call: {failure}"));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(call.try_terminal().is_none());
    assert!(call.recover_after_driver_shutdown().is_none());
}
