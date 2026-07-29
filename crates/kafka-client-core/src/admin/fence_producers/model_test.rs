//! Scenarios for bounded producer-fencing request validation.

use super::{
    AdminFenceProducersPlan, AdminFenceProducersPlanError,
    FENCE_PRODUCERS_MAX_TRANSACTIONAL_ID_BYTES, FENCE_PRODUCERS_MAX_TRANSACTIONAL_IDS,
};

#[test]
fn plan_preserves_unique_transactional_ids_in_caller_order() {
    let plan =
        AdminFenceProducersPlan::new(vec!["invoice-worker".to_owned(), "audit-writer".to_owned()])
            .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.transactional_ids(), ["invoice-worker", "audit-writer"]);
}

#[test]
fn plan_rejects_empty_oversized_and_duplicate_identities() {
    for (ids, expected) in [
        (
            Vec::new(),
            AdminFenceProducersPlanError::EmptyTransactionalIdBatch,
        ),
        (
            vec![String::new()],
            AdminFenceProducersPlanError::EmptyTransactionalId,
        ),
        (
            vec!["x".repeat(i16::MAX as usize + 1)],
            AdminFenceProducersPlanError::TransactionalIdTooLong,
        ),
        (
            vec!["same".to_owned(), "same".to_owned()],
            AdminFenceProducersPlanError::DuplicateTransactionalId,
        ),
    ] {
        assert_eq!(AdminFenceProducersPlan::new(ids), Err(expected));
    }
}

#[test]
fn plan_bounds_identity_count_and_aggregate_bytes() {
    let too_many = (0..=FENCE_PRODUCERS_MAX_TRANSACTIONAL_IDS)
        .map(|index| format!("transaction-{index}"))
        .collect();
    assert_eq!(
        AdminFenceProducersPlan::new(too_many),
        Err(AdminFenceProducersPlanError::TooManyTransactionalIds)
    );

    let identity = "x".repeat(i16::MAX as usize - 8);
    let count = FENCE_PRODUCERS_MAX_TRANSACTIONAL_ID_BYTES / identity.len() + 1;
    let too_many_bytes = (0..count)
        .map(|index| format!("{index:07}-{identity}"))
        .collect();
    assert_eq!(
        AdminFenceProducersPlan::new(too_many_bytes),
        Err(AdminFenceProducersPlanError::TransactionalIdBytesExceeded)
    );
}
