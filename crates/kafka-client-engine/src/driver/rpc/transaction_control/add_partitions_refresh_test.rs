//! Causal `AddPartitions` coordinator-refresh call lifecycle scenarios.

use std::time::{Duration, Instant};

use kafka_driver::CompletionError;

use crate::EngineConfig;

use super::{
    super::super::DriverOwner, TransactionAddPartitionsCall, TransactionAddPartitionsPoll,
    TransactionPartitionTarget,
};

#[test]
fn accepted_call_yields_one_closed_completion_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let mut call = TransactionAddPartitionsCall::submit(
        &driver,
        "writer",
        42,
        7,
        vec![TransactionPartitionTarget::new("orders".into(), 2)],
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    drop(driver);

    assert!(matches!(
        call.poll(),
        TransactionAddPartitionsPoll::Terminal(Err(CompletionError::Closed))
    ));
    assert!(matches!(call.poll(), TransactionAddPartitionsPoll::Pending));
    call.discard_after_driver_shutdown();
}
