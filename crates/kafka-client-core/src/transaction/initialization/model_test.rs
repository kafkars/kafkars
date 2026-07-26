//! Scalar transaction initialization model boundaries.

use super::{
    TransactionInitializationPlan, TransactionInitializationPlanError, TransactionalOwnerId,
    TransactionalProducerIdentity, model::MAX_TRANSACTION_TIMEOUT_MS,
};

#[test]
fn timeout_plan_accepts_only_kafka_signed_positive_milliseconds() {
    assert_eq!(
        TransactionInitializationPlan::new(0),
        Err(TransactionInitializationPlanError::ZeroTimeout)
    );
    assert_eq!(
        TransactionInitializationPlan::new(MAX_TRANSACTION_TIMEOUT_MS + 1),
        Err(TransactionInitializationPlanError::TimeoutTooLarge)
    );
    assert_eq!(
        TransactionInitializationPlan::new(MAX_TRANSACTION_TIMEOUT_MS)
            .map(TransactionInitializationPlan::transaction_timeout_ms),
        Ok(MAX_TRANSACTION_TIMEOUT_MS)
    );
}

#[test]
fn owner_and_transactional_identity_preserve_exact_scalars() {
    let owner = TransactionalOwnerId::from_raw(u64::MAX);
    assert_eq!(owner.get(), u64::MAX);

    assert_eq!(TransactionalProducerIdentity::try_new(-1, 0), None);
    assert_eq!(TransactionalProducerIdentity::try_new(0, -1), None);
    let identity = TransactionalProducerIdentity::try_new(i64::MAX, i16::MAX)
        .unwrap_or_else(|| panic!("nonnegative identity"));
    assert_eq!(identity.producer_id(), i64::MAX);
    assert_eq!(identity.producer_epoch(), i16::MAX);
}
