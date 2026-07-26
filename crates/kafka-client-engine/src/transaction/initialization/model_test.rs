//! Transaction identity and broker-timeout request ownership.

use super::TransactionInitializationRequest;

#[test]
fn request_keeps_identity_and_broker_timeout_distinct() {
    let request = TransactionInitializationRequest::new("invoice-writer".to_owned(), 45_000);
    assert_eq!(request.transactional_id(), "invoice-writer");
    assert_eq!(request.transaction_timeout_ms(), 45_000);
    assert_eq!(request.into_parts(), ("invoice-writer".to_owned(), 45_000));
}
