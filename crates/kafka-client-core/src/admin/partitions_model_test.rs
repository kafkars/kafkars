//! Scenarios for validated automatic-assignment partition increases.

use super::{CreatePartitionsPlan, CreatePartitionsPlanError, CreatePartitionsSpecification};

fn topic(name: &str, count: i32) -> CreatePartitionsSpecification {
    CreatePartitionsSpecification::new(name.to_owned(), count)
}

#[test]
fn plan_preserves_order_and_rejects_ambiguous_or_invalid_topics() {
    let plan = CreatePartitionsPlan::new(vec![topic("orders", 8), topic("audit", 4)], true)
        .unwrap_or_else(|error| panic!("valid CreatePartitions plan: {error}"));
    assert_eq!(plan.topics()[0].topic(), "orders");
    assert_eq!(plan.topics()[0].total_count(), 8);
    assert!(plan.validate_only());
    assert_eq!(
        CreatePartitionsPlan::new(Vec::new(), false),
        Err(CreatePartitionsPlanError::EmptyBatch)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("", 2)], false),
        Err(CreatePartitionsPlanError::EmptyTopicName)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("orders", 0)], false),
        Err(CreatePartitionsPlanError::InvalidTotalCount)
    );
    assert_eq!(
        CreatePartitionsPlan::new(vec![topic("orders", 2), topic("orders", 3)], false),
        Err(CreatePartitionsPlanError::DuplicateTopic)
    );
}
