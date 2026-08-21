//! Public opaque listing and exact broker-error accessors.

use super::{ListTransactionsBrokerError, TransactionListing};

#[test]
fn listing_preserves_signed_producer_and_unknown_state() {
    let listing = TransactionListing::new(
        "invoice-worker".to_owned(),
        i64::MIN,
        "FutureBrokerState".to_owned(),
    );
    assert_eq!(listing.transactional_id(), "invoice-worker");
    assert_eq!(listing.producer_id(), i64::MIN);
    assert_eq!(listing.transaction_state(), "FutureBrokerState");

    let error = ListTransactionsBrokerError::new(7, -32_000);
    assert_eq!(error.broker_id(), 7);
    assert_eq!(error.code(), -32_000);
}
