//! Scenarios for validated `DescribeTopics` semantic input.

use super::{DescribeTopicsPlan, DescribeTopicsPlanError, DescribeTopicsSelection};

#[test]
fn plan_preserves_order_and_rejects_ambiguous_topics() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics plan: {error}"));
    assert!(!plan.include_authorized_operations());
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
fn authorized_operations_are_explicit_and_default_false_for_names_and_ids() {
    let named = DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid named plan: {error}"))
        .with_authorized_operations(true);
    let by_id = DescribeTopicsPlan::by_ids(vec![[1; 16]])
        .unwrap_or_else(|error| panic!("valid topic-ID plan: {error}"))
        .with_authorized_operations(true);
    assert!(named.include_authorized_operations());
    assert!(by_id.include_authorized_operations());
    assert!(!DescribeTopicsPlan::all(false).include_authorized_operations());
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

#[test]
fn topic_id_plan_preserves_order_and_rejects_protocol_sentinels_or_duplicates() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DescribeTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID plan: {error}"));
    assert_eq!(
        plan.selection(),
        &DescribeTopicsSelection::Ids(vec![first, second])
    );
    assert_eq!(
        DescribeTopicsPlan::by_ids(Vec::new()),
        Err(DescribeTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        DescribeTopicsPlan::by_ids(vec![[0; 16]]),
        Err(DescribeTopicsPlanError::ZeroTopicId)
    );
    assert_eq!(
        DescribeTopicsPlan::by_ids(vec![first, first]),
        Err(DescribeTopicsPlanError::DuplicateTopicId)
    );
}
