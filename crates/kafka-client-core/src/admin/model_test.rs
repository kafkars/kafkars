//! Scenarios for validated ordered `CreateTopics` request facts.

use super::{CreateTopicConfig, CreateTopicSpecification, CreateTopicsPlan, CreateTopicsPlanError};

fn topic(name: &str) -> CreateTopicSpecification {
    CreateTopicSpecification::new(
        name,
        3,
        2,
        vec![
            CreateTopicConfig::new("cleanup.policy", Some("compact".to_owned())),
            CreateTopicConfig::new("retention.ms", None),
        ],
    )
}

#[test]
fn plan_preserves_topic_and_nullable_config_order() {
    let result = CreateTopicsPlan::new(vec![topic("orders"), topic("audit")], true);
    assert!(result.is_ok());
    let Ok(plan) = result else {
        return;
    };

    assert!(plan.validate_only());
    assert_eq!(plan.topics()[0].name(), "orders");
    assert_eq!(plan.topics()[1].name(), "audit");
    assert_eq!(plan.topics()[0].configs()[0].name(), "cleanup.policy");
    assert_eq!(plan.topics()[0].configs()[0].value(), Some("compact"));
    assert_eq!(plan.topics()[0].configs()[1].value(), None);
}

#[test]
fn plan_rejects_empty_duplicate_and_invalid_topic_facts() {
    assert_eq!(
        CreateTopicsPlan::new(Vec::new(), false),
        Err(CreateTopicsPlanError::EmptyBatch)
    );
    assert_eq!(
        CreateTopicsPlan::new(vec![topic("orders"), topic("orders")], false),
        Err(CreateTopicsPlanError::DuplicateTopic)
    );
    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::new("orders", 0, 1, Vec::new())],
            false,
        ),
        Err(CreateTopicsPlanError::InvalidPartitionCount)
    );
    assert_eq!(
        CreateTopicsPlan::new(
            vec![CreateTopicSpecification::new("orders", 1, 0, Vec::new())],
            false,
        ),
        Err(CreateTopicsPlanError::InvalidReplicationFactor)
    );
}
