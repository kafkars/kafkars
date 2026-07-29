//! Scenarios for validated ordered `DeleteTopics` request facts.

use super::delete_model::DeleteTopicsSelection;
use super::{DeleteTopicsPlan, DeleteTopicsPlanError};

#[test]
fn plan_preserves_order_and_rejects_ambiguous_names() {
    let plan = DeleteTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DeleteTopics plan: {error}"));
    assert_eq!(plan.topics(), &["orders".to_owned(), "audit".to_owned()]);
    assert_eq!(
        DeleteTopicsPlan::new(Vec::new()),
        Err(DeleteTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        DeleteTopicsPlan::new(vec![String::new()]),
        Err(DeleteTopicsPlanError::EmptyTopicName)
    );
    assert_eq!(
        DeleteTopicsPlan::new(vec!["orders".to_owned(), "orders".to_owned()]),
        Err(DeleteTopicsPlanError::DuplicateTopic)
    );
}

#[test]
fn topic_id_plan_preserves_order_and_rejects_ambiguous_ids() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID DeleteTopics plan: {error}"));
    assert_eq!(
        plan.selection(),
        &DeleteTopicsSelection::Ids(vec![first, second])
    );
    assert_eq!(plan.topic_ids(), &[first, second]);
    assert!(plan.topics().is_empty());
    assert_eq!(
        DeleteTopicsPlan::by_ids(Vec::new()),
        Err(DeleteTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        DeleteTopicsPlan::by_ids(vec![[0; 16]]),
        Err(DeleteTopicsPlanError::ZeroTopicId)
    );
    assert_eq!(
        DeleteTopicsPlan::by_ids(vec![first, first]),
        Err(DeleteTopicsPlanError::DuplicateTopicId)
    );
}
