//! Cooperative-sticky Join subscription metadata scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupTiming, ClassicProtocol, GroupAssignmentPartition,
    PartitionIndex, TopicId,
};
use kafka_wire::decode_consumer_protocol_subscription;
use kafka_wire_core::DecodeLimits;

use super::{
    ClassicJoinRequestFailure, ClassicSyncTopic, classic_join_group_request_with_instance,
    validation::{COOPERATIVE_STICKY_PROTOCOL, COOPERATIVE_SUBSCRIPTION_VERSION},
};

#[test]
fn request_encodes_v2_owned_partitions_and_generation() {
    let orders = TopicId::from_raw(7);
    let payments = TopicId::from_raw(9);
    let owned = [
        GroupAssignmentPartition::new(orders, PartitionIndex::from_raw(1)),
        GroupAssignmentPartition::new(orders, PartitionIndex::from_raw(2)),
        GroupAssignmentPartition::new(payments, PartitionIndex::from_raw(0)),
    ];
    let owned_topics = [
        ClassicSyncTopic::new(orders, Arc::from("orders")),
        ClassicSyncTopic::new(payments, Arc::from("payments")),
    ];
    let generation =
        ClassicGeneration::try_from_raw(12).unwrap_or_else(|| panic!("nonnegative generation"));
    let prepared = classic_join_group_request_with_instance(
        "workers",
        Some("member-a"),
        None,
        ClassicProtocol::CooperativeSticky,
        &topics(&["orders", "payments"]),
        &owned,
        &owned_topics,
        Some(generation),
        timing(),
    )
    .unwrap_or_else(|error| panic!("cooperative Join request failed: {error:?}"));
    let protocol = &prepared.request_for_test().protocols[0];
    assert_eq!(protocol.name.as_str(), COOPERATIVE_STICKY_PROTOCOL);
    let (version, subscription) =
        decode_consumer_protocol_subscription(protocol.metadata.clone(), DecodeLimits::default())
            .unwrap_or_else(|error| panic!("subscription decode failed: {error}"));
    assert_eq!(version.value(), COOPERATIVE_SUBSCRIPTION_VERSION);
    assert_eq!(subscription.generation_id, 12);
    assert_eq!(subscription.owned_partitions.len(), 2);
    assert_eq!(subscription.owned_partitions[0].topic.as_str(), "orders");
    assert_eq!(subscription.owned_partitions[0].partitions, [1, 2]);
    assert_eq!(subscription.owned_partitions[1].topic.as_str(), "payments");
    assert_eq!(subscription.owned_partitions[1].partitions, [0]);
}

#[test]
fn initial_request_uses_v2_unknown_generation() {
    let prepared = classic_join_group_request_with_instance(
        "workers",
        None,
        None,
        ClassicProtocol::CooperativeSticky,
        &topics(&["orders"]),
        &[],
        &[],
        None,
        timing(),
    )
    .unwrap_or_else(|error| panic!("initial cooperative Join failed: {error:?}"));
    let (version, subscription) = decode_consumer_protocol_subscription(
        prepared.request_for_test().protocols[0].metadata.clone(),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("subscription decode failed: {error}"));
    assert_eq!(version.value(), COOPERATIVE_SUBSCRIPTION_VERSION);
    assert_eq!(subscription.generation_id, -1);
    assert!(subscription.owned_partitions.is_empty());
}

#[test]
fn request_reports_owned_partitions_from_a_dropped_topic() {
    let orders = TopicId::from_raw(7);
    let owned = [GroupAssignmentPartition::new(
        orders,
        PartitionIndex::from_raw(1),
    )];
    let owned_topics = [ClassicSyncTopic::new(orders, Arc::from("orders"))];
    let prepared = classic_join_group_request_with_instance(
        "workers",
        Some("member-a"),
        None,
        ClassicProtocol::CooperativeSticky,
        &topics(&["payments"]),
        &owned,
        &owned_topics,
        ClassicGeneration::try_from_raw(12),
        timing(),
    )
    .unwrap_or_else(|error| panic!("dropped-topic Join request failed: {error:?}"));
    let (_, subscription) = decode_consumer_protocol_subscription(
        prepared.request_for_test().protocols[0].metadata.clone(),
        DecodeLimits::default(),
    )
    .unwrap_or_else(|error| panic!("subscription decode failed: {error}"));

    assert_eq!(subscription.topics.len(), 1);
    assert_eq!(subscription.topics[0].as_str(), "payments");
    assert_eq!(subscription.owned_partitions.len(), 1);
    assert_eq!(subscription.owned_partitions[0].topic.as_str(), "orders");
    assert_eq!(subscription.owned_partitions[0].partitions, [1]);
}

#[test]
fn range_rejects_cooperative_only_facts() {
    let generation =
        ClassicGeneration::try_from_raw(1).unwrap_or_else(|| panic!("nonnegative generation"));
    assert_eq!(
        classic_join_group_request_with_instance(
            "workers",
            None,
            None,
            ClassicProtocol::Range,
            &topics(&["orders"]),
            &[],
            &[],
            Some(generation),
            timing(),
        )
        .err(),
        Some(ClassicJoinRequestFailure::UnexpectedGeneration)
    );
}

#[test]
fn owned_partition_shape_is_validated_before_encoding() {
    let topic = TopicId::from_raw(7);
    let mapping = [ClassicSyncTopic::new(topic, Arc::from("orders"))];
    let duplicate = [
        GroupAssignmentPartition::new(topic, PartitionIndex::from_raw(1)),
        GroupAssignmentPartition::new(topic, PartitionIndex::from_raw(1)),
    ];
    assert_eq!(
        request_with_owned(&duplicate, &mapping).err(),
        Some(ClassicJoinRequestFailure::DuplicateOwnedPartition)
    );
    let out_of_order = [
        GroupAssignmentPartition::new(topic, PartitionIndex::from_raw(2)),
        GroupAssignmentPartition::new(topic, PartitionIndex::from_raw(1)),
    ];
    assert_eq!(
        request_with_owned(&out_of_order, &mapping).err(),
        Some(ClassicJoinRequestFailure::OutOfOrderOwnedPartition)
    );
    let missing = [GroupAssignmentPartition::new(
        TopicId::from_raw(9),
        PartitionIndex::from_raw(0),
    )];
    assert_eq!(
        request_with_owned(&missing, &mapping).err(),
        Some(ClassicJoinRequestFailure::MissingOwnedTopic(
            TopicId::from_raw(9)
        ))
    );
    let too_large = [GroupAssignmentPartition::new(
        topic,
        PartitionIndex::from_raw(i32::MAX as u32 + 1),
    )];
    assert_eq!(
        request_with_owned(&too_large, &mapping).err(),
        Some(ClassicJoinRequestFailure::OwnedPartitionOutOfRange(
            i32::MAX as u32 + 1
        ))
    );
}

fn request_with_owned(
    owned: &[GroupAssignmentPartition],
    mapping: &[ClassicSyncTopic],
) -> Result<super::PreparedClassicJoinGroupRequest, ClassicJoinRequestFailure> {
    classic_join_group_request_with_instance(
        "workers",
        Some("member-a"),
        None,
        ClassicProtocol::CooperativeSticky,
        &topics(&["orders"]),
        owned,
        mapping,
        None,
        timing(),
    )
}

fn topics(values: &[&str]) -> Vec<Arc<str>> {
    values.iter().copied().map(Arc::from).collect()
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("timing failed: {error}"))
}
