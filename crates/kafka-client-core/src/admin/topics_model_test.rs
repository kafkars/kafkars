//! Scenarios for validated `DescribeTopics` semantic input.

use super::{DescribeTopicsPlan, DescribeTopicsPlanError, DescribeTopicsSelection};

#[test]
fn plan_preserves_order_and_rejects_ambiguous_topics() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"));
    assert!(matches!(
        plan.selection(),
        DescribeTopicsSelection::Named(topics)
            if topics.iter().map(String::as_str).eq(["orders", "audit"])
    ));
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

#[test]
fn all_topics_is_explicit_and_retains_internal_policy() {
    assert_eq!(
        DescribeTopicsPlan::all(false).selection(),
        &DescribeTopicsSelection::All {
            include_internal: false,
        }
    );
    assert_eq!(
        DescribeTopicsPlan::all(true).selection(),
        &DescribeTopicsSelection::All {
            include_internal: true,
        }
    );
}
