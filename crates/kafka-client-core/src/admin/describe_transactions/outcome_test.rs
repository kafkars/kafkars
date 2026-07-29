//! Scenarios for exact per-ID transaction-description outcomes.

use core::num::NonZeroI16;

use super::{
    AdminDescribeTransactionBrokerError, AdminDescribeTransactionDescription,
    AdminDescribeTransactionOutcome, AdminDescribeTransactionResult,
    AdminDescribeTransactionsBatch,
};

#[test]
fn success_preserves_one_stable_transaction_description() {
    let outcome = AdminDescribeTransactionOutcome::described(
        "invoice-worker".to_owned(),
        AdminDescribeTransactionDescription::new(
            "Ongoing".to_owned(),
            60_000,
            Some(19),
            91,
            7,
            Vec::new(),
        ),
    );

    assert_eq!(outcome.transactional_id(), "invoice-worker");
    let AdminDescribeTransactionResult::Described(description) = outcome.result() else {
        panic!("ID must be described");
    };
    assert_eq!(description.transaction_state(), "Ongoing");
}

#[test]
fn broker_failure_retains_the_exact_signed_code_without_inventing_a_diagnostic() {
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let outcome = AdminDescribeTransactionOutcome::broker_failed(
        "audit-writer".to_owned(),
        AdminDescribeTransactionBrokerError::new(code),
    );

    let AdminDescribeTransactionResult::BrokerFailed(error) = outcome.result() else {
        panic!("ID must retain broker rejection");
    };
    assert_eq!(error.code(), -31_777);
}

#[test]
fn batch_preserves_caller_order_and_maximum_throttle() {
    let description = || {
        AdminDescribeTransactionDescription::new("Empty".to_owned(), -1, None, -1, -1, Vec::new())
    };
    let batch = AdminDescribeTransactionsBatch::new(
        73,
        vec![
            AdminDescribeTransactionOutcome::described("invoice-worker".to_owned(), description()),
            AdminDescribeTransactionOutcome::described("audit-writer".to_owned(), description()),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].transactional_id(), "invoice-worker");
    assert_eq!(batch.outcomes()[1].transactional_id(), "audit-writer");
}
