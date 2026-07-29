//! Scenarios for bounded transaction-description request validation.

use super::{
    AdminDescribeTransactionsPlan, AdminDescribeTransactionsPlanError,
    DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES, DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_IDS,
};

#[test]
fn plan_preserves_unique_transactional_ids_in_caller_order() {
    let plan = AdminDescribeTransactionsPlan::new(vec![
        "invoice-worker".to_owned(),
        "audit-writer".to_owned(),
    ])
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.transactional_ids(), ["invoice-worker", "audit-writer"]);
}

#[test]
fn plan_rejects_empty_oversized_and_duplicate_identities() {
    for (ids, expected) in [
        (
            Vec::new(),
            AdminDescribeTransactionsPlanError::EmptyTransactionalIdBatch,
        ),
        (
            vec![String::new()],
            AdminDescribeTransactionsPlanError::EmptyTransactionalId,
        ),
        (
            vec!["x".repeat(i16::MAX as usize + 1)],
            AdminDescribeTransactionsPlanError::TransactionalIdTooLong,
        ),
        (
            vec!["same".to_owned(), "same".to_owned()],
            AdminDescribeTransactionsPlanError::DuplicateTransactionalId,
        ),
    ] {
        assert_eq!(AdminDescribeTransactionsPlan::new(ids), Err(expected));
    }
}

#[test]
fn plan_bounds_identity_count_and_aggregate_bytes() {
    let too_many = (0..=DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_IDS)
        .map(|index| format!("transaction-{index}"))
        .collect();
    assert_eq!(
        AdminDescribeTransactionsPlan::new(too_many),
        Err(AdminDescribeTransactionsPlanError::TooManyTransactionalIds)
    );

    let identity = "x".repeat(i16::MAX as usize - 8);
    let count = DESCRIBE_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES / identity.len() + 1;
    let too_many_bytes = (0..count)
        .map(|index| format!("{index:07}-{identity}"))
        .collect();
    assert_eq!(
        AdminDescribeTransactionsPlan::new(too_many_bytes),
        Err(AdminDescribeTransactionsPlanError::TransactionalIdBytesExceeded)
    );
}
