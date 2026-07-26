//! Scenarios for exact consumer-group offset alteration result values.

use core::num::NonZeroI16;

use super::{
    AlterConsumerGroupOffsetBrokerError, AlterConsumerGroupOffsetOutcome,
    AlterConsumerGroupOffsetResult, AlterConsumerGroupOffsetsBatch,
};

#[test]
fn partition_failure_retains_identity_and_exact_signed_code() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let outcome = AlterConsumerGroupOffsetOutcome::failed(
        "audit".to_owned(),
        3,
        AlterConsumerGroupOffsetBrokerError::new(code),
    );

    assert_eq!(outcome.topic(), "audit");
    assert_eq!(outcome.partition(), 3);
    let AlterConsumerGroupOffsetResult::Failed(error) = outcome.result() else {
        panic!("partition must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
}

#[test]
fn response_batch_retains_throttle_and_caller_order() {
    let batch = AlterConsumerGroupOffsetsBatch::new(
        73,
        vec![
            AlterConsumerGroupOffsetOutcome::altered("orders".to_owned(), 2),
            AlterConsumerGroupOffsetOutcome::altered("audit".to_owned(), 0),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 73);
    assert_eq!(outcomes.len(), 2);
}
