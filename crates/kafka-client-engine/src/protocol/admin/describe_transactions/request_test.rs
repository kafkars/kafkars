//! Exact one-ID API-key 65 request construction scenarios.

use super::describe_transactions_request;

#[test]
fn request_contains_exactly_the_selected_transactional_id() {
    let request = describe_transactions_request("invoice-worker");

    assert_eq!(request.transactional_ids.len(), 1);
    assert_eq!(request.transactional_ids[0].as_str(), "invoice-worker");
    assert!(request.unknown_tagged_fields.is_empty());
}
