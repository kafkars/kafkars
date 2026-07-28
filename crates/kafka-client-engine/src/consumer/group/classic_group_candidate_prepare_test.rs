//! Atomic member, topic, and cursor preparation for classic-group cycles.

use std::sync::Arc;

use kafka_client_core::{GroupId, JoinedMemberSlot, MembershipCycle};

use super::{
    classic_group_candidate::{ClassicGroupCycleCandidateError, JoinedGroupMember},
    classic_group_owner::ClassicGroupOwner,
    classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

fn group_id() -> GroupId {
    GroupId::try_from_raw(5).unwrap_or_else(|| panic!("nonzero group identity"))
}

fn slot(value: u32) -> JoinedMemberSlot {
    JoinedMemberSlot::try_from_raw(value).unwrap_or_else(|| panic!("nonzero member slot"))
}

fn catalog() -> GroupSessionCatalog {
    GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[Arc::from("orders")])
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

#[test]
fn dropped_follower_candidate_does_not_advance_member_or_topic_cursors() {
    let catalog = catalog();
    let member_cursor = catalog.next_member_id;
    let topic_cursor = catalog.next_topic_id;
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));

    assert_eq!(
        candidate.local_member_id(),
        member_cursor.unwrap_or_else(|| panic!("member"))
    );
    drop(candidate);
    assert_eq!(catalog.next_member_id, member_cursor);
    assert_eq!(catalog.next_topic_id, topic_cursor);
    assert_eq!(catalog.retained_topic_count(), 1);
}

#[test]
fn member_id_required_spelling_reuses_the_staged_catalog_identity() {
    let mut catalog = catalog();
    let cycle = MembershipCycle::initial();
    let required = catalog
        .prepare_required_join_member(cycle, Arc::from("assigned-member"))
        .unwrap_or_else(|error| panic!("required member preparation: {error:?}"));
    let required_id = required.member_id;
    catalog.commit_required_join_member(required);

    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("assigned-member"))
        .unwrap_or_else(|error| panic!("replacement candidate: {error:?}"));
    assert_eq!(candidate.local_member_id(), required_id);
    assert_eq!(
        candidate
            .next_member_id_after_install()
            .map(kafka_client_core::MemberId::get),
        required_id.get().checked_add(1)
    );
    assert!(matches!(
        catalog.prepare_follower_cycle(cycle, Arc::from("different-member")),
        Err(ClassicGroupCycleCandidateError::RequiredMemberMismatch)
    ));
}

#[test]
fn invalid_leader_membership_is_atomic() {
    let catalog = catalog();
    let mismatch = catalog.prepare_leader_cycle(
        MembershipCycle::initial(),
        Arc::from("local"),
        vec![JoinedGroupMember::new(
            slot(1),
            Arc::from("local"),
            vec![Arc::from("payments")],
        )],
    );
    assert!(matches!(
        mismatch,
        Err(ClassicGroupCycleCandidateError::LocalSubscriptionMismatch)
    ));

    let duplicate = catalog.prepare_leader_cycle(
        MembershipCycle::initial(),
        Arc::from("local"),
        vec![
            JoinedGroupMember::new(slot(1), Arc::from("local"), vec![Arc::from("orders")]),
            JoinedGroupMember::new(slot(2), Arc::from("local"), vec![Arc::from("orders")]),
        ],
    );
    assert!(matches!(
        duplicate,
        Err(ClassicGroupCycleCandidateError::DuplicateMember)
    ));
    assert_eq!(catalog.retained_topic_count(), 1);
    assert_eq!(
        catalog.next_member_id.map(kafka_client_core::MemberId::get),
        Some(1)
    );
}

#[test]
fn exhausted_member_cursor_preserves_joining_machine_and_catalog() {
    let mut catalog = catalog();
    catalog.set_identity_cursors_for_test(None, catalog.next_topic_id);
    let mut owner = ClassicGroupOwner::new(
        group_id(),
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let cycle = classic_group_test_support::begin(&mut owner);
    let phase = owner.machine().phase();
    let topic_cursor = catalog.next_topic_id;
    let retained_bytes = catalog.retained_topic_name_bytes();

    assert!(matches!(
        catalog.prepare_follower_cycle(cycle, Arc::from("member")),
        Err(ClassicGroupCycleCandidateError::MemberIdentityExhausted)
    ));
    assert_eq!(owner.machine().phase(), phase);
    assert_eq!(owner.machine().active_cycle(), Some(cycle));
    assert!(owner.pending().is_none());
    assert_eq!(catalog.next_member_id, None);
    assert_eq!(catalog.next_topic_id, topic_cursor);
    assert_eq!(catalog.retained_topic_name_bytes(), retained_bytes);
}

#[test]
fn exhausted_foreign_topic_cursor_preserves_staged_members_and_owner() {
    let mut catalog = catalog();
    let member_cursor = catalog.next_member_id;
    let retained_bytes = catalog.retained_topic_name_bytes();
    catalog.set_identity_cursors_for_test(member_cursor, None);
    let mut owner = ClassicGroupOwner::new(
        group_id(),
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let cycle = classic_group_test_support::begin(&mut owner);

    let result = catalog.prepare_leader_cycle(
        cycle,
        Arc::from("local"),
        vec![
            JoinedGroupMember::new(slot(1), Arc::from("local"), vec![Arc::from("orders")]),
            JoinedGroupMember::new(slot(2), Arc::from("remote"), vec![Arc::from("payments")]),
        ],
    );
    assert!(matches!(
        result,
        Err(ClassicGroupCycleCandidateError::TopicIdentityExhausted)
    ));
    assert_eq!(owner.machine().active_cycle(), Some(cycle));
    assert!(owner.pending().is_none());
    assert_eq!(catalog.next_member_id, member_cursor);
    assert_eq!(catalog.next_topic_id, None);
    assert_eq!(catalog.topic_id("payments"), None);
    assert_eq!(catalog.retained_topic_name_bytes(), retained_bytes);
}
