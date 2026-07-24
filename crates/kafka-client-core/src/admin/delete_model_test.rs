//! Scenarios for validated ordered `DeleteTopics` request facts.

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
