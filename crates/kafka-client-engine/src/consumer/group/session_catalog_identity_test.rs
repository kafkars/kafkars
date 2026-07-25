//! Group-session identity exhaustion and retained-binding scenarios.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId, MemberId, PartitionIndex, TopicId};

use super::session_catalog::{
    GroupSessionCatalog, GroupSessionCatalogError, GroupSessionPartition,
};

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value)
        .unwrap_or_else(|| panic!("test assignment generation must be nonzero"))
}

fn catalog() -> GroupSessionCatalog {
    let group_id =
        GroupId::try_from_raw(19).unwrap_or_else(|| panic!("test group identity must be nonzero"));
    GroupSessionCatalog::try_new(group_id, Arc::from("group"))
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

fn partition(topic: &str, index: u32) -> GroupSessionPartition {
    GroupSessionPartition::new(Arc::from(topic), PartitionIndex::from_raw(index))
}

#[test]
fn member_identity_exhaustion_does_not_install_a_session() {
    let mut catalog = catalog();
    catalog.set_identity_cursors_for_test(None, Some(TopicId::from_raw(1)));
    assert!(matches!(
        catalog.prepare_replacement(Arc::from("member"), 0, generation(1), Vec::new()),
        Err(GroupSessionCatalogError::MemberIdentityExhausted)
    ));
    assert!(catalog.live_assignment().is_none());
    assert_eq!(catalog.retained_topic_count(), 0);
}

#[test]
fn exhausted_topic_cursor_still_allows_retained_binding_reuse() {
    let mut catalog = catalog();
    catalog.set_identity_cursors_for_test(
        MemberId::try_from_raw(1),
        Some(TopicId::from_raw(u64::MAX)),
    );
    catalog
        .prepare_replacement(
            Arc::from("member-1"),
            1,
            generation(1),
            vec![partition("last", 0)],
        )
        .unwrap_or_else(|error| panic!("last topic identity failed: {error:?}"))
        .commit();
    let first_member = catalog.current_member_id();
    assert!(matches!(
        catalog.prepare_replacement(
            Arc::from("member-2"),
            2,
            generation(2),
            vec![partition("overflow", 0)]
        ),
        Err(GroupSessionCatalogError::TopicIdentityExhausted)
    ));
    assert_eq!(catalog.current_member_id(), first_member);
    assert_eq!(catalog.classic_generation(), Some(1));

    catalog
        .prepare_replacement(
            Arc::from("member-2"),
            2,
            generation(2),
            vec![partition("last", 8)],
        )
        .unwrap_or_else(|error| panic!("retained topic reuse failed: {error:?}"))
        .commit();
    let assignment = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("assignment expected"));
    assert_eq!(
        assignment.partitions()[0].topic_id(),
        TopicId::from_raw(u64::MAX)
    );
    assert_eq!(assignment.partitions()[0].partition().get(), 8);
}
