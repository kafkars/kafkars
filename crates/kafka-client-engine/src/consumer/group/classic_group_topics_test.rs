//! Candidate-local topic staging capacity and cursor failure scenarios.

use std::sync::Arc;

use kafka_client_core::{GroupId, TopicId};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidateError,
    classic_group_topics::PreparedCycleTopics,
    session_catalog::{GroupSessionCatalog, MAX_GROUP_SESSION_TOPICS},
};

fn catalog(topics: &[Arc<str>]) -> GroupSessionCatalog {
    let group_id = GroupId::try_from_raw(3).unwrap_or_else(|| panic!("nonzero group identity"));
    GroupSessionCatalog::try_new(group_id, Arc::from("workers"), topics)
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

#[test]
fn exhausted_topic_cursor_leaves_catalog_unchanged_and_reuses_durable_topics() {
    let mut catalog = catalog(&[Arc::from("orders")]);
    catalog.set_identity_cursors_for_test(catalog.next_member_id, None);
    let mut staged = PreparedCycleTopics::new(&catalog);

    assert_eq!(
        staged
            .translate_subscription(vec![Arc::from("orders")])
            .unwrap_or_else(|error| panic!("durable topic should reuse: {error:?}")),
        vec![TopicId::from_raw(1)]
    );
    assert!(matches!(
        staged.translate_subscription(vec![Arc::from("payments")]),
        Err(ClassicGroupCycleCandidateError::TopicIdentityExhausted)
    ));
    assert_eq!(catalog.topic_id("payments"), None);
    assert_eq!(catalog.retained_topic_count(), 1);
    assert_eq!(catalog.next_topic_id, None);
}

#[test]
fn retained_topic_capacity_failure_does_not_mutate_the_catalog() {
    let topics = (0..MAX_GROUP_SESSION_TOPICS)
        .map(|index| Arc::from(format!("topic-{index:02}")))
        .collect::<Vec<Arc<str>>>();
    let catalog = catalog(&topics);
    let before_bytes = catalog.retained_topic_name_bytes();
    let before_cursor = catalog.next_topic_id;
    let mut staged = PreparedCycleTopics::new(&catalog);

    assert!(matches!(
        staged.translate_subscription(vec![Arc::from("overflow")]),
        Err(ClassicGroupCycleCandidateError::TopicCapacity {
            actual,
            limit: MAX_GROUP_SESSION_TOPICS,
        }) if actual == MAX_GROUP_SESSION_TOPICS + 1
    ));
    assert_eq!(catalog.retained_topic_count(), MAX_GROUP_SESSION_TOPICS);
    assert_eq!(catalog.retained_topic_name_bytes(), before_bytes);
    assert_eq!(catalog.next_topic_id, before_cursor);
}
