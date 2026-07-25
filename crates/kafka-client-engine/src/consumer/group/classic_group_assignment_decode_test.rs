//! Catalog identity, local-subscription, ordering, and lossless-failure scenarios.

use std::sync::Arc;

use kafka_client_core::{GroupId, MembershipCycle, PartitionIndex};

use crate::protocol::consumer::{CLASSIC_SYNC_MAX_MEMBER_PARTITIONS, NamedAssignmentPartition};

use super::{
    classic_group_assignment_decode::{
        ClassicGroupAssignmentDecodeError, decode_classic_group_assignment,
    },
    session_catalog::GroupSessionCatalog,
};

fn group_id() -> GroupId {
    GroupId::try_from_raw(31).unwrap_or_else(|| panic!("nonzero group identity"))
}

fn catalog(topics: &[&str]) -> GroupSessionCatalog {
    let topics = topics
        .iter()
        .map(|topic| Arc::<str>::from(*topic))
        .collect::<Vec<_>>();
    GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &topics)
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

fn named(topic: &str, partition: i32) -> NamedAssignmentPartition {
    NamedAssignmentPartition::from_assignment_decode_parts_for_test(Arc::from(topic), partition)
}

#[test]
fn follower_assignment_is_canonicalized_by_catalog_identity() {
    let catalog = catalog(&["payments", "orders"]);
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));

    let decoded = decode_classic_group_assignment(
        &catalog,
        &candidate,
        vec![named("payments", 2), named("orders", 3), named("orders", 1)],
    )
    .unwrap_or_else(|failure| panic!("assignment decode failed: {:?}", failure.kind()));
    let orders = catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity"));
    let payments = catalog
        .topic_id("payments")
        .unwrap_or_else(|| panic!("payments identity"));

    assert_eq!(
        decoded
            .iter()
            .map(|partition| (partition.topic_id(), partition.partition().get()))
            .collect::<Vec<_>>(),
        [(orders, 1), (orders, 3), (payments, 2)]
    );
}

#[test]
fn unknown_topic_rejection_retains_input_and_both_identity_owners() {
    let catalog = catalog(&["orders"]);
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));
    let member_cursor = catalog.next_member_id;
    let topic_cursor = catalog.next_topic_id;
    let retained_topics = catalog.retained_topic_count();
    let candidate_member = candidate.local_member_id();

    let failure = decode_classic_group_assignment(&catalog, &candidate, vec![named("payments", 0)])
        .err()
        .unwrap_or_else(|| panic!("unknown topic must reject"));

    assert_eq!(
        failure.kind(),
        ClassicGroupAssignmentDecodeError::UnknownTopic { entry: 0 }
    );
    assert_eq!(failure.partitions()[0].topic(), "payments");
    assert_eq!(catalog.next_member_id, member_cursor);
    assert_eq!(catalog.next_topic_id, topic_cursor);
    assert_eq!(catalog.retained_topic_count(), retained_topics);
    assert_eq!(candidate.local_member_id(), candidate_member);
}

#[test]
fn negative_partition_rejection_is_lossless_and_checked_before_core() {
    let catalog = catalog(&["orders"]);
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));

    let failure = decode_classic_group_assignment(&catalog, &candidate, vec![named("orders", -1)])
        .err()
        .unwrap_or_else(|| panic!("negative partition must reject"));
    assert_eq!(
        failure.kind(),
        ClassicGroupAssignmentDecodeError::NegativePartition {
            entry: 0,
            partition: -1,
        }
    );
    let (kind, retained) = failure.into_parts();
    assert!(matches!(
        kind,
        ClassicGroupAssignmentDecodeError::NegativePartition { .. }
    ));
    assert_eq!(retained[0].partition(), -1);
}

#[test]
fn duplicate_partition_rejection_reports_the_canonical_identity() {
    let catalog = catalog(&["orders"]);
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));
    let topic_id = catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity"));

    let failure = decode_classic_group_assignment(
        &catalog,
        &candidate,
        vec![named("orders", 4), named("orders", 4)],
    )
    .err()
    .unwrap_or_else(|| panic!("duplicate partition must reject"));

    assert_eq!(
        failure.kind(),
        ClassicGroupAssignmentDecodeError::DuplicatePartition {
            topic_id,
            partition: PartitionIndex::from_raw(4),
        }
    );
    assert_eq!(failure.partitions().len(), 2);
}

#[test]
fn assignment_capacity_is_the_protocol_owned_bound() {
    let catalog = catalog(&["orders"]);
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("follower candidate failed: {error:?}"));
    let upper = i32::try_from(CLASSIC_SYNC_MAX_MEMBER_PARTITIONS)
        .unwrap_or_else(|error| panic!("protocol partition bound must fit i32: {error}"));
    let partitions = (0..=upper)
        .map(|partition| named("orders", partition))
        .collect::<Vec<_>>();

    let failure = decode_classic_group_assignment(&catalog, &candidate, partitions)
        .err()
        .unwrap_or_else(|| panic!("oversized assignment must reject"));
    assert_eq!(
        failure.kind(),
        ClassicGroupAssignmentDecodeError::PartitionCapacity {
            actual: CLASSIC_SYNC_MAX_MEMBER_PARTITIONS + 1,
            limit: CLASSIC_SYNC_MAX_MEMBER_PARTITIONS,
        }
    );
    assert_eq!(
        failure.partitions().len(),
        CLASSIC_SYNC_MAX_MEMBER_PARTITIONS + 1
    );
}
