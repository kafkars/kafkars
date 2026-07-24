//! Atomic reassignment scenarios for retained topic identities and name bytes.

use std::sync::Arc;

use kafka_client_core::{AssignedPartition, PartitionIndex, StartPosition, TopicId};

use super::assigned_topics::{
    AssignedPartitionInput, AssignedTopicLimits, AssignedTopics, AssignedTopicsError,
};

#[derive(Debug, Eq, PartialEq)]
struct OwnerSnapshot {
    next_topic_id: Option<TopicId>,
    retained_topics: usize,
    retained_name_bytes: usize,
    partitions: Vec<AssignedPartition>,
}

fn limits(
    max_unique_topics: usize,
    max_partitions: usize,
    max_topic_name_bytes: usize,
    max_retained_name_bytes: usize,
) -> AssignedTopicLimits {
    AssignedTopicLimits::new(
        max_unique_topics,
        max_partitions,
        max_topic_name_bytes,
        max_retained_name_bytes,
    )
}

fn entry(topic: &str, partition: u32) -> AssignedPartitionInput {
    AssignedPartitionInput::new(
        Arc::from(topic),
        PartitionIndex::from_raw(partition),
        StartPosition::Beginning,
    )
}

fn owner(topic: &str, limits: AssignedTopicLimits) -> AssignedTopics {
    AssignedTopics::from_initial_for_test(vec![entry(topic, 0)], limits)
        .unwrap_or_else(|error| panic!("initial assignment failed: {error:?}"))
}

fn snapshot(owner: &AssignedTopics) -> OwnerSnapshot {
    OwnerSnapshot {
        next_topic_id: owner.next_topic_id_for_test(),
        retained_topics: owner.retained_topic_count(),
        retained_name_bytes: owner.retained_name_bytes(),
        partitions: owner.partitions().to_vec(),
    }
}

fn commit_replacement(
    owner: &mut AssignedTopics,
    replacement: Vec<AssignedPartitionInput>,
) -> Result<(), AssignedTopicsError> {
    owner.prepare_replacement(replacement)?.commit();
    Ok(())
}

fn assert_atomic_failure(
    owner: &mut AssignedTopics,
    replacement: Vec<AssignedPartitionInput>,
    expected: AssignedTopicsError,
) {
    let before = snapshot(owner);
    let old_id = before.partitions[0].partition().topic_id();
    let old_name = Arc::clone(
        owner
            .name(old_id)
            .unwrap_or_else(|error| panic!("old topic lookup failed: {error:?}")),
    );

    let actual = match owner.prepare_replacement(replacement) {
        Ok(_prepared) => panic!("replacement should fail during staging"),
        Err(error) => error,
    };
    assert_eq!(actual, expected);
    assert_eq!(snapshot(owner), before);
    let retained = owner
        .name(old_id)
        .unwrap_or_else(|error| panic!("retained topic lookup failed: {error:?}"));
    assert!(Arc::ptr_eq(retained, &old_name));
    if let Some(uncommitted_id) = before.next_topic_id {
        assert_eq!(
            owner.name(uncommitted_id),
            Err(AssignedTopicsError::UnknownTopic(uncommitted_id))
        );
    }
}

#[test]
fn replacement_retains_old_bindings_and_never_reuses_their_ids() {
    let orders: Arc<str> = Arc::from("orders");
    let mut owner = AssignedTopics::from_initial_for_test(
        vec![AssignedPartitionInput::new(
            Arc::clone(&orders),
            PartitionIndex::from_raw(3),
            StartPosition::End,
        )],
        limits(4, 3, 16, 64),
    )
    .unwrap_or_else(|error| panic!("initial assignment failed: {error:?}"));
    let orders_id = owner.partitions()[0].partition().topic_id();

    commit_replacement(&mut owner, vec![entry("payments", 8), entry("payments", 2)])
        .unwrap_or_else(|error| panic!("replacement failed: {error:?}"));

    let partitions = owner.partitions();
    let payments_id = partitions[0].partition().topic_id();
    assert_ne!(payments_id, orders_id);
    assert_eq!(payments_id, partitions[1].partition().topic_id());
    assert_eq!(partitions[0].partition().partition().get(), 8);
    assert_eq!(partitions[1].partition().partition().get(), 2);
    assert_eq!(owner.retained_topic_count(), 2);
    assert_eq!(
        owner.retained_name_bytes(),
        "orders".len() + "payments".len()
    );
    let retained_orders = owner
        .name(orders_id)
        .unwrap_or_else(|error| panic!("old topic lookup failed: {error:?}"));
    assert!(Arc::ptr_eq(retained_orders, &orders));

    commit_replacement(&mut owner, vec![entry("orders", 5)])
        .unwrap_or_else(|error| panic!("old-name replacement failed: {error:?}"));
    assert_eq!(owner.partitions()[0].partition().topic_id(), orders_id);
    assert_eq!(owner.next_topic_id_for_test(), Some(TopicId::from_raw(3)));
}

#[test]
fn partition_and_unique_topic_count_failures_are_atomic() {
    let mut partition_limited = owner("orders", limits(4, 1, 16, 64));
    assert_atomic_failure(
        &mut partition_limited,
        vec![entry("orders", 1), entry("orders", 2)],
        AssignedTopicsError::PartitionCapacity {
            actual: 2,
            limit: 1,
        },
    );

    let mut topic_limited = owner("orders", limits(1, 2, 16, 64));
    assert_atomic_failure(
        &mut topic_limited,
        vec![entry("payments", 1)],
        AssignedTopicsError::RetainedTopicCapacity {
            actual: 2,
            limit: 1,
        },
    );
}

#[test]
fn per_name_and_cumulative_name_byte_failures_are_atomic() {
    let mut name_limited = owner("a", limits(4, 2, 1, 64));
    assert_atomic_failure(
        &mut name_limited,
        vec![entry("é", 1)],
        AssignedTopicsError::TopicNameBytes {
            actual: "é".len(),
            limit: 1,
        },
    );

    let mut bytes_limited = owner("ab", limits(4, 2, 16, 4));
    assert_atomic_failure(
        &mut bytes_limited,
        vec![entry("cde", 1)],
        AssignedTopicsError::RetainedNameBytes {
            actual: 5,
            limit: 4,
        },
    );
}

#[test]
fn identity_exhaustion_is_atomic_while_retained_names_remain_usable() {
    let mut owner = AssignedTopics::from_initial_with_next_for_test(
        vec![entry("last", 0)],
        limits(4, 2, 16, 64),
        Some(TopicId::from_raw(u64::MAX)),
    )
    .unwrap_or_else(|error| panic!("last identity assignment failed: {error:?}"));
    assert_eq!(owner.next_topic_id_for_test(), None);

    assert_atomic_failure(
        &mut owner,
        vec![entry("overflow", 1)],
        AssignedTopicsError::TopicIdentityExhausted,
    );
    commit_replacement(&mut owner, vec![entry("last", 7)])
        .unwrap_or_else(|error| panic!("retained-name replacement failed: {error:?}"));
    assert_eq!(
        owner.partitions()[0].partition().topic_id(),
        TopicId::from_raw(u64::MAX)
    );
}
