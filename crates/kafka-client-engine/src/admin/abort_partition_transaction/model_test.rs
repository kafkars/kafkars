//! Engine request conversion tests for one partition transaction abort.

use kafka_client_core::AbortPartitionTransactionPlanError;

use super::AbortPartitionTransactionRequest;

#[test]
fn preserves_every_specification_scalar() {
    let plan = AbortPartitionTransactionRequest::new("orders".to_owned(), 3, 41, 7, 11)
        .into_plan()
        .expect("valid request");

    assert_eq!(plan.topic(), "orders");
    assert_eq!(plan.partition(), 3);
    assert_eq!(plan.producer_id(), 41);
    assert_eq!(plan.producer_epoch(), 7);
    assert_eq!(plan.coordinator_epoch(), 11);
    assert_eq!(plan.transaction_version(), 0);
    assert_eq!(plan.minimum_api_version(), 1);
}

#[test]
fn preserves_explicit_positive_transaction_version_and_version_floor() {
    for transaction_version in [1, 2, i8::MAX] {
        let plan = AbortPartitionTransactionRequest::new("orders".to_owned(), 3, 41, 7, 11)
            .with_transaction_version(transaction_version)
            .into_plan()
            .expect("valid request");

        assert_eq!(plan.transaction_version(), transaction_version);
        assert_eq!(plan.minimum_api_version(), 2);
    }
}

#[test]
fn negative_transaction_version_remains_inert_until_core_admission() {
    let request = AbortPartitionTransactionRequest::new("orders".to_owned(), 3, 41, 7, 11)
        .with_transaction_version(-1);

    assert_eq!(
        request.into_plan(),
        Err(AbortPartitionTransactionPlanError::NegativeTransactionVersion)
    );
}
