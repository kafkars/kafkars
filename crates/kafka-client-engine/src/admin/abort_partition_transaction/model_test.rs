//! Engine request conversion tests for one partition transaction abort.

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
}
