//! Atomic replacement, monotonic identity, and rollback scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, MemberId, PartitionIndex, TopicId,
};

use super::session_catalog::{
    GroupSessionCatalog, GroupSessionCatalogError, GroupSessionPartition,
};

#[derive(Debug, Eq, PartialEq)]
struct Snapshot {
    next_member_id: Option<MemberId>,
    next_topic_id: Option<TopicId>,
    retained_topics: usize,
    retained_topic_name_bytes: usize,
    member_id: Option<MemberId>,
    member: Option<Arc<str>>,
    classic_generation: Option<i32>,
    assignment_generation: Option<AssignmentGeneration>,
    partitions: Vec<GroupAssignmentPartition>,
}

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value)
        .unwrap_or_else(|| panic!("test assignment generation must be nonzero"))
}

fn group_id(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("test group identity must be nonzero"))
}

fn partition(topic: &str, index: u32) -> GroupSessionPartition {
    GroupSessionPartition::new(Arc::from(topic), PartitionIndex::from_raw(index))
}

fn snapshot(catalog: &GroupSessionCatalog) -> Snapshot {
    Snapshot {
        next_member_id: catalog.next_member_id,
        next_topic_id: catalog.next_topic_id,
        retained_topics: catalog.retained_topic_count(),
        retained_topic_name_bytes: catalog.retained_topic_name_bytes(),
        member_id: catalog.current_member_id(),
        member: catalog.current_member().map(Arc::clone),
        classic_generation: catalog.classic_generation(),
        assignment_generation: catalog.assignment_generation(),
        partitions: catalog
            .live_assignment()
            .map_or_else(Vec::new, |assignment| assignment.partitions().to_vec()),
    }
}

fn catalog() -> GroupSessionCatalog {
    GroupSessionCatalog::try_new(group_id(19), Arc::from("group"))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

#[test]
fn dropping_prepared_replacement_changes_nothing() {
    let mut catalog = catalog();
    catalog
        .prepare_replacement(
            Arc::from("member-1"),
            1,
            generation(1),
            vec![partition("orders", 0)],
        )
        .unwrap_or_else(|error| panic!("initial replacement failed: {error:?}"))
        .commit();
    let before = snapshot(&catalog);
    let old_topic = catalog.topic_name(TopicId::from_raw(1)).map_or_else(
        |error| panic!("old topic lookup failed: {error:?}"),
        Arc::clone,
    );

    let prepared = catalog
        .prepare_replacement(
            Arc::from("member-2"),
            2,
            generation(2),
            vec![partition("payments", 7)],
        )
        .unwrap_or_else(|error| panic!("staging failed: {error:?}"));
    assert_eq!(
        prepared.member_id(),
        MemberId::try_from_raw(2)
            .unwrap_or_else(|| panic!("second member identity must be nonzero"))
    );
    drop(prepared);

    assert_eq!(snapshot(&catalog), before);
    assert!(Arc::ptr_eq(
        catalog
            .topic_name(TopicId::from_raw(1))
            .unwrap_or_else(|error| panic!("old topic lookup failed: {error:?}")),
        &old_topic
    ));
    assert_eq!(
        catalog.topic_name(TopicId::from_raw(2)),
        Err(GroupSessionCatalogError::UnknownTopic(TopicId::from_raw(2)))
    );
}

#[test]
fn committed_replacements_never_reuse_member_or_topic_identities() {
    let mut catalog = catalog();
    catalog
        .prepare_replacement(
            Arc::from("same-member-spelling"),
            1,
            generation(1),
            vec![partition("orders", 0)],
        )
        .unwrap_or_else(|error| panic!("first replacement failed: {error:?}"))
        .commit();
    let first_member = catalog
        .current_member_id()
        .unwrap_or_else(|| panic!("first member identity expected"));
    let orders = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("assignment expected"))
        .partitions()[0]
        .topic_id();

    catalog
        .prepare_replacement(
            Arc::from("same-member-spelling"),
            2,
            generation(2),
            vec![partition("payments", 0)],
        )
        .unwrap_or_else(|error| panic!("second replacement failed: {error:?}"))
        .commit();
    let second_member = catalog
        .current_member_id()
        .unwrap_or_else(|| panic!("second member identity expected"));
    let payments = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("assignment expected"))
        .partitions()[0]
        .topic_id();
    assert!(second_member > first_member);
    assert!(payments > orders);

    catalog
        .prepare_replacement(
            Arc::from("member-3"),
            3,
            generation(3),
            vec![partition("orders", 4)],
        )
        .unwrap_or_else(|error| panic!("third replacement failed: {error:?}"))
        .commit();
    assert_eq!(
        catalog
            .live_assignment()
            .unwrap_or_else(|| panic!("assignment expected"))
            .partitions()[0]
            .topic_id(),
        orders
    );
    assert_eq!(catalog.retained_topic_count(), 2);
}

#[test]
fn failed_staging_is_atomic_across_cursors_maps_bytes_and_assignment() {
    let mut catalog = catalog();
    catalog
        .prepare_replacement(
            Arc::from("member-1"),
            1,
            generation(1),
            vec![partition("orders", 0)],
        )
        .unwrap_or_else(|error| panic!("initial replacement failed: {error:?}"))
        .commit();
    let before = snapshot(&catalog);
    let result = catalog.prepare_replacement(
        Arc::from("member-2"),
        2,
        generation(2),
        vec![partition("payments", 1), partition("payments", 1)],
    );
    assert!(matches!(
        result,
        Err(GroupSessionCatalogError::Assignment(_))
    ));
    assert_eq!(snapshot(&catalog), before);
}
