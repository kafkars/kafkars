//! Fixed-assignment topic ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{NextFetchOffset, PartitionIndex, StartPosition, TopicId};

use super::assigned_topics::{
    AssignedPartitionInput, AssignedTopicLimits, AssignedTopics, AssignedTopicsError,
};

fn entry(topic: Arc<str>, partition: u32, start: StartPosition) -> AssignedPartitionInput {
    AssignedPartitionInput::new(topic, PartitionIndex::from_raw(partition), start)
}

const fn limits(max_unique_topics: usize, max_partitions: usize) -> AssignedTopicLimits {
    AssignedTopicLimits::new(max_unique_topics, max_partitions, 256, 4_096)
}

#[test]
fn duplicate_names_share_one_stable_identity_and_exact_owned_name() {
    let orders: Arc<str> = Arc::from("orders");
    let assignment = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::clone(&orders), 3, StartPosition::Beginning),
            entry(Arc::from("orders"), 1, StartPosition::End),
            entry(Arc::from("payments"), 0, StartPosition::Beginning),
        ],
        limits(2, 3),
    )
    .unwrap_or_else(|error| panic!("assignment failed: {error:?}"));

    let partitions = assignment.partitions();
    let orders_id = partitions[0].partition().topic_id();
    assert_eq!(orders_id, partitions[1].partition().topic_id());
    assert_ne!(orders_id, partitions[2].partition().topic_id());
    let retained = assignment
        .name(orders_id)
        .unwrap_or_else(|error| panic!("topic lookup failed: {error:?}"));
    assert_eq!(retained.as_ref(), "orders");
    assert!(Arc::ptr_eq(retained, &orders));
}

#[test]
fn caller_partition_order_and_start_positions_are_preserved() {
    let offset =
        NextFetchOffset::try_from_raw(41).unwrap_or_else(|| panic!("test offset should be valid"));
    let assignment = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::from("z"), 8, StartPosition::End),
            entry(Arc::from("a"), 2, StartPosition::Offset(offset)),
            entry(Arc::from("z"), 5, StartPosition::Beginning),
        ],
        limits(2, 3),
    )
    .unwrap_or_else(|error| panic!("assignment failed: {error:?}"));

    let partitions = assignment.partitions();
    assert_eq!(partitions[0].partition().partition().get(), 8);
    assert_eq!(partitions[0].start(), StartPosition::End);
    assert_eq!(partitions[1].partition().partition().get(), 2);
    assert_eq!(partitions[1].start(), StartPosition::Offset(offset));
    assert_eq!(partitions[2].partition().partition().get(), 5);
    assert_eq!(partitions[2].start(), StartPosition::Beginning);
}

#[test]
fn unique_topic_and_partition_capacities_are_independently_bounded() {
    let duplicate_names = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::from("orders"), 0, StartPosition::Beginning),
            entry(Arc::from("orders"), 1, StartPosition::Beginning),
        ],
        limits(1, 2),
    );
    assert!(duplicate_names.is_ok());

    let topics = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::from("orders"), 0, StartPosition::Beginning),
            entry(Arc::from("payments"), 0, StartPosition::Beginning),
        ],
        limits(1, 2),
    );
    assert!(matches!(
        topics,
        Err(AssignedTopicsError::RetainedTopicCapacity {
            actual: 2,
            limit: 1,
        })
    ));

    let partitions = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::from("orders"), 0, StartPosition::Beginning),
            entry(Arc::from("orders"), 1, StartPosition::Beginning),
        ],
        limits(1, 1),
    );
    assert!(matches!(
        partitions,
        Err(AssignedTopicsError::PartitionCapacity {
            actual: 2,
            limit: 1,
        })
    ));
}

#[test]
fn identity_exhaustion_and_unknown_lookup_are_explicit() {
    let exhausted = AssignedTopics::from_initial_with_next_for_test(
        vec![
            entry(Arc::from("last"), 0, StartPosition::Beginning),
            entry(Arc::from("overflow"), 0, StartPosition::Beginning),
        ],
        limits(2, 2),
        Some(TopicId::from_raw(u64::MAX)),
    );
    assert!(matches!(
        exhausted,
        Err(AssignedTopicsError::TopicIdentityExhausted)
    ));

    let assignment = AssignedTopics::from_initial_for_test(
        vec![entry(Arc::from("orders"), 0, StartPosition::Beginning)],
        limits(1, 1),
    )
    .unwrap_or_else(|error| panic!("assignment failed: {error:?}"));
    assert_eq!(
        assignment.name(TopicId::from_raw(99)),
        Err(AssignedTopicsError::UnknownTopic(TopicId::from_raw(99)))
    );
}

#[test]
fn topic_validation_and_duplicate_partition_policy_are_not_reimplemented() {
    let assignment = AssignedTopics::from_initial_for_test(
        vec![
            entry(Arc::from(""), 7, StartPosition::Beginning),
            entry(Arc::from(""), 7, StartPosition::End),
        ],
        limits(1, 2),
    )
    .unwrap_or_else(|error| panic!("assignment facts failed: {error:?}"));

    assert_eq!(assignment.partitions().len(), 2);
    let topic_id = assignment.partitions()[0].partition().topic_id();
    assert_eq!(assignment.name(topic_id).map(AsRef::as_ref), Ok(""));
}
