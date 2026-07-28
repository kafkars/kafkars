//! Linear call-wrapper completion and shutdown-recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan};
use kafka_driver::CompletionError;

use crate::{
    EngineConfig,
    driver::{DriverOwner, GroupOffsetAlterCall},
};

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let mut call = GroupOffsetAlterCall::submit(
        &driver,
        expected.clone(),
        usize::MAX,
        8_192,
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
        .unwrap_or_else(|_call| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(&expected, usize::MAX, 8_192));
    recovered.seal();
}

#[test]
fn zero_result_capacity_returns_exact_definitely_unsent_evidence() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = plan();
    let rejection = match GroupOffsetAlterCall::submit(
        &driver,
        expected.clone(),
        4_096,
        0,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero terminal capacity must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(rejection.into_submission_evidence(), (expected, 4_096, 0));
}

fn plan() -> AlterConsumerGroupOffsetsPlan {
    AlterConsumerGroupOffsetsPlan::new(
        "readers".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            91,
            None,
            None,
        )],
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
}
