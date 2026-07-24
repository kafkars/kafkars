//! Scenarios for validated `DescribeTopics` semantic input.

use super::{DescribeTopicsPlan, DescribeTopicsPlanError};

#[test]
fn plan_preserves_order_and_rejects_ambiguous_topics() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"));
    assert_eq!(plan.topics(), ["orders", "audit"]);
    assert_eq!(
        DescribeTopicsPlan::new(Vec::new()),
        Err(DescribeTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        DescribeTopicsPlan::new(vec![String::new()]),
        Err(DescribeTopicsPlanError::EmptyTopicName)
    );
    assert_eq!(
        DescribeTopicsPlan::new(vec!["orders".to_owned(), "orders".to_owned()]),
        Err(DescribeTopicsPlanError::DuplicateTopic)
    );
}
