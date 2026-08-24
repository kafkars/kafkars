//! Atomic incremental topic-catalog preparation scenarios.

use std::sync::Arc;

use kafka_client_core::{NextFetchOffset, PartitionIndex, StartPosition};

use super::{AssignedPartitionInput, AssignedTopicLimits, AssignedTopics};
use crate::consumer::assigned_host::AssignedConsumerPartition;

#[test]
fn additions_append_and_removals_preserve_survivor_order_and_topic_identity() {
    let mut topics = AssignedTopics::from_initial_for_test(
        vec![entry("orders", 0), entry("payments", 0)],
        limits(),
    )
    .unwrap_or_else(|error| panic!("initial catalog: {error:?}"));
    let orders_id = topics.partitions()[0].partition().topic_id();
    let payments_id = topics.partitions()[1].partition().topic_id();

    let addition = topics
        .prepare_addition(vec![entry("shipments", 2), entry("orders", 1)])
        .unwrap_or_else(|error| panic!("prepare addition: {error:?}"));
    assert_eq!(addition.added().len(), 2);
    addition.commit();
    assert_eq!(
        partition_shape(&topics),
        vec![
            (orders_id, 0),
            (payments_id, 0),
            (topics.partitions()[2].partition().topic_id(), 2),
            (orders_id, 1)
        ]
    );

    let removal = topics
        .prepare_removal(&[target("payments", 0), target("orders", 1)])
        .unwrap_or_else(|error| panic!("prepare removal: {error:?}"));
    assert_eq!(removal.removed()[0].topic_id(), payments_id);
    removal.commit();
    assert_eq!(partition_shape(&topics).len(), 2);
    assert_eq!(partition_shape(&topics)[0], (orders_id, 0));

    let readd = topics
        .prepare_addition(vec![entry("payments", 3)])
        .unwrap_or_else(|error| panic!("prepare re-addition: {error:?}"));
    assert_eq!(readd.added()[0].partition().topic_id(), payments_id);
    readd.commit();
    assert_eq!(partition_shape(&topics).last(), Some(&(payments_id, 3)));
}

#[test]
fn dropped_incremental_tokens_leave_catalog_unchanged() {
    let mut topics = AssignedTopics::from_initial_for_test(vec![entry("orders", 0)], limits())
        .unwrap_or_else(|error| panic!("initial catalog: {error:?}"));
    let before = partition_shape(&topics);
    let retained = topics.retained_topic_count();

    drop(
        topics
            .prepare_addition(vec![entry("payments", 1)])
            .unwrap_or_else(|error| panic!("prepare addition: {error:?}")),
    );
    assert_eq!(partition_shape(&topics), before);
    assert_eq!(topics.retained_topic_count(), retained);

    drop(
        topics
            .prepare_removal(&[target("orders", 0)])
            .unwrap_or_else(|error| panic!("prepare removal: {error:?}")),
    );
    assert_eq!(partition_shape(&topics), before);
}

fn entry(topic: &str, partition: u32) -> AssignedPartitionInput {
    AssignedPartitionInput::new(
        Arc::from(topic),
        PartitionIndex::from_raw(partition),
        StartPosition::Offset(
            NextFetchOffset::try_from_raw(i64::from(partition))
                .unwrap_or_else(|| panic!("nonnegative offset")),
        ),
    )
}

fn target(topic: &str, partition: i32) -> AssignedConsumerPartition {
    AssignedConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("valid target: {error}"))
}

fn limits() -> AssignedTopicLimits {
    AssignedTopicLimits::new(8, 8, 249, 128)
}

fn partition_shape(topics: &AssignedTopics) -> Vec<(kafka_client_core::TopicId, u32)> {
    topics
        .partitions()
        .iter()
        .map(|partition| {
            (
                partition.partition().topic_id(),
                partition.partition().partition().get(),
            )
        })
        .collect()
}
