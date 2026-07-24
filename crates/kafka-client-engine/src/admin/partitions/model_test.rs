//! Canonical bounded-storage scenarios for `CreatePartitions` request values.

use super::{CreatePartitionsRequest, PartitionIncrease};

#[test]
fn canonicalization_preserves_order_counts_and_validate_only() {
    let mut orders = String::with_capacity(64);
    orders.push_str("orders");
    let mut topics = Vec::with_capacity(8);
    topics.push(PartitionIncrease::new(orders, 8));
    topics.push(PartitionIncrease::new("audit", 4));
    let request = CreatePartitionsRequest::new(topics)
        .with_validate_only(true)
        .canonicalize();
    assert!(request.storage_is_canonical());
    let plan = request
        .into_plan()
        .unwrap_or_else(|error| panic!("valid partition plan: {error}"));
    assert_eq!(plan.topics()[0].topic(), "orders");
    assert_eq!(plan.topics()[0].total_count(), 8);
    assert!(plan.validate_only());
}
