//! Scenarios for exact per-target producer-description outcomes.

use core::num::NonZeroI16;

use super::{
    AdminDescribeProducerBrokerError, AdminDescribeProducerOutcome, AdminDescribeProducerResult,
    AdminDescribeProducersBatch, AdminProducerState,
};

#[test]
fn success_preserves_zero_or_more_stable_producer_facts() {
    let outcome = AdminDescribeProducerOutcome::described(
        "orders".to_owned(),
        2,
        vec![AdminProducerState::new(91, 7, 42, 5, 3, None)],
    );

    assert_eq!(outcome.topic(), "orders");
    assert_eq!(outcome.partition(), 2);
    let AdminDescribeProducerResult::Described(producers) = outcome.result() else {
        panic!("target must be described");
    };
    assert_eq!(producers[0].producer_id(), 91);
}

#[test]
fn broker_failure_retains_exact_signed_code_and_bounded_diagnostic_shape() {
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let outcome = AdminDescribeProducerOutcome::broker_failed(
        "audit".to_owned(),
        0,
        AdminDescribeProducerBrokerError::new(code, Some("denied".to_owned()), true),
    );

    let AdminDescribeProducerResult::BrokerFailed(error) = outcome.result() else {
        panic!("target must retain broker rejection");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("denied"));
    assert!(error.message_truncated());
}

#[test]
fn batch_preserves_caller_order_and_maximum_throttle() {
    let batch = AdminDescribeProducersBatch::new(
        73,
        vec![
            AdminDescribeProducerOutcome::described("orders".to_owned(), 2, Vec::new()),
            AdminDescribeProducerOutcome::described("audit".to_owned(), 0, Vec::new()),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}
