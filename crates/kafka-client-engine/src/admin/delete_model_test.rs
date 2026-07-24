//! Canonical bounded-storage scenarios for public `DeleteTopics` request values.

use super::DeleteTopicsRequest;

#[test]
fn canonicalization_preserves_order_and_removes_spare_capacity() {
    let mut orders = String::with_capacity(64);
    orders.push_str("orders");
    let mut topics = Vec::with_capacity(8);
    topics.push(orders);
    topics.push("audit".to_owned());
    let request = DeleteTopicsRequest::new(topics).canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid deletion plan: {error}"));
    assert_eq!(plan.topics(), &["orders".to_owned(), "audit".to_owned()]);
}
