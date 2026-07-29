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
    assert_eq!(plan.topics()[0].replica_assignments(), None);
    assert!(plan.validate_only());
}

#[test]
fn explicit_assignments_are_canonical_ordered_and_charged() {
    let mut first = Vec::with_capacity(8);
    first.extend([3, 1]);
    let mut second = Vec::with_capacity(8);
    second.extend([2, 4]);
    let mut assignments = Vec::with_capacity(8);
    assignments.extend([first, second]);
    let mut topics = Vec::with_capacity(8);
    topics.push(PartitionIncrease::with_replica_assignments(
        "orders",
        8,
        assignments,
    ));
    let explicit = CreatePartitionsRequest::new(topics).canonicalize();
    let automatic =
        CreatePartitionsRequest::new(vec![PartitionIncrease::new("orders", 8)]).canonicalize();

    assert!(explicit.storage_is_canonical());
    assert!(
        explicit
            .retained_charge()
            .unwrap_or_else(|| panic!("small explicit request fits"))
            > automatic
                .retained_charge()
                .unwrap_or_else(|| panic!("small automatic request fits"))
    );
    let plan = explicit
        .into_plan()
        .unwrap_or_else(|error| panic!("valid explicit partition plan: {error}"));
    assert_eq!(
        plan.topics()[0].replica_assignments(),
        Some(&[vec![3, 1], vec![2, 4]][..])
    );
}
