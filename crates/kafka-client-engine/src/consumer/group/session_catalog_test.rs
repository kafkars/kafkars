//! Atomic local-subscription registration and durable topic identity scenarios.

use std::sync::Arc;

use kafka_client_core::{GroupId, TopicId};

use super::session_catalog::{
    GroupSessionCatalog, GroupSessionCatalogError, MAX_GROUP_SESSION_TOPIC_BYTES,
    MAX_GROUP_SESSION_TOPICS, MAX_KAFKA_GROUP_STRING_BYTES,
};

fn group_id() -> GroupId {
    GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"))
}

#[test]
fn registration_retains_one_bounded_ordered_local_subscription() {
    let orders: Arc<str> = Arc::from("orders");
    let payments: Arc<str> = Arc::from("payments");
    let catalog = GroupSessionCatalog::try_new(
        group_id(),
        Arc::from("workers"),
        &[Arc::clone(&payments), Arc::clone(&orders)],
    )
    .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));

    assert_eq!(
        catalog.local_subscription(),
        &[TopicId::from_raw(1), TopicId::from_raw(2)]
    );
    assert_eq!(catalog.topic_id("orders"), Some(TopicId::from_raw(1)));
    assert_eq!(catalog.topic_id("payments"), Some(TopicId::from_raw(2)));
    assert!(Arc::ptr_eq(
        catalog
            .topic_name(TopicId::from_raw(1))
            .unwrap_or_else(|error| panic!("topic lookup failed: {error:?}")),
        &orders
    ));
    assert!(Arc::ptr_eq(
        catalog
            .topic_name(TopicId::from_raw(2))
            .unwrap_or_else(|error| panic!("topic lookup failed: {error:?}")),
        &payments
    ));
    assert_eq!(catalog.retained_topic_count(), 2);
    assert_eq!(
        catalog.retained_topic_name_bytes(),
        "orders".len() + "payments".len()
    );
    assert_eq!(catalog.next_topic_id, Some(TopicId::from_raw(3)));
    assert!(catalog.live_assignment().is_none());
}

#[test]
fn registration_rejects_invalid_or_duplicate_topics_without_a_catalog() {
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[Arc::from("")]),
        Err(GroupSessionCatalogError::EmptyTopic)
    ));
    assert!(matches!(
        GroupSessionCatalog::try_new(
            group_id(),
            Arc::from("workers"),
            &[Arc::from("orders"), Arc::from("orders")],
        ),
        Err(GroupSessionCatalogError::DuplicateTopic)
    ));
    let oversized: Arc<str> = Arc::from("t".repeat(MAX_GROUP_SESSION_TOPIC_BYTES + 1));
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[oversized]),
        Err(GroupSessionCatalogError::TopicBytes { .. })
    ));
    let too_many = (0..=MAX_GROUP_SESSION_TOPICS)
        .map(|index| Arc::from(format!("topic-{index}")))
        .collect::<Vec<Arc<str>>>();
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &too_many),
        Err(GroupSessionCatalogError::RetainedTopicCapacity {
            actual,
            limit: MAX_GROUP_SESSION_TOPICS,
        }) if actual == MAX_GROUP_SESSION_TOPICS + 1
    ));
}

#[test]
fn group_spelling_remains_exact_and_bounded() {
    let group: Arc<str> = Arc::from("invoice-workers");
    let catalog = GroupSessionCatalog::try_new(group_id(), Arc::clone(&group), &[])
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    assert!(Arc::ptr_eq(catalog.group(), &group));
    assert_eq!(catalog.group_id(), group_id());
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(), Arc::from(""), &[]),
        Err(GroupSessionCatalogError::EmptyGroup)
    ));
    let oversized: Arc<str> = Arc::from("g".repeat(MAX_KAFKA_GROUP_STRING_BYTES + 1));
    assert!(matches!(
        GroupSessionCatalog::try_new(group_id(), oversized, &[]),
        Err(GroupSessionCatalogError::GroupBytes { .. })
    ));
}

#[test]
fn empty_local_subscription_is_a_valid_dormant_registration() {
    let catalog = GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[])
        .unwrap_or_else(|error| panic!("empty subscription should be valid: {error:?}"));

    assert!(catalog.local_subscription().is_empty());
    assert_eq!(catalog.retained_topic_count(), 0);
    assert_eq!(catalog.retained_topic_name_bytes(), 0);
    assert_eq!(catalog.next_topic_id, Some(TopicId::from_raw(1)));
    assert!(catalog.live_assignment().is_none());
}
