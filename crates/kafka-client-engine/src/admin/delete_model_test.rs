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

#[test]
fn topic_id_request_preserves_order_and_canonicalizes_its_batch() {
    let first = [1; 16];
    let second = [2; 16];
    let mut topic_ids = Vec::with_capacity(8);
    topic_ids.extend([first, second]);
    let request = DeleteTopicsRequest::by_ids(topic_ids).canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    assert_eq!(plan.topic_ids(), &[first, second]);
}
