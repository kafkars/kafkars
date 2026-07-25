//! Exhausted classic-group identity cursor scenarios.

use std::sync::Arc;

use kafka_client_core::{GroupId, MemberId, MembershipCycle, TopicId};

use super::{
    classic_group_candidate::ClassicGroupCycleCandidateError, session_catalog::GroupSessionCatalog,
};

fn catalog() -> GroupSessionCatalog {
    let group_id = GroupId::try_from_raw(19).unwrap_or_else(|| panic!("nonzero group identity"));
    GroupSessionCatalog::try_new(group_id, Arc::from("group"), &[Arc::from("orders")])
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}

#[test]
fn exhausted_member_cursor_rejects_without_topic_or_assignment_mutation() {
    let mut catalog = catalog();
    catalog.set_identity_cursors_for_test(None, Some(TopicId::from_raw(2)));

    assert!(matches!(
        catalog.prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member")),
        Err(ClassicGroupCycleCandidateError::MemberIdentityExhausted)
    ));
    assert_eq!(catalog.retained_topic_count(), 1);
    assert_eq!(catalog.topic_id("orders"), Some(TopicId::from_raw(1)));
    assert!(catalog.live_assignment().is_none());
}

#[test]
fn final_member_identity_can_be_staged_without_wrapping() {
    let mut catalog = catalog();
    catalog.set_identity_cursors_for_test(
        MemberId::try_from_raw(u64::MAX),
        Some(TopicId::from_raw(2)),
    );
    let candidate = catalog
        .prepare_follower_cycle(MembershipCycle::initial(), Arc::from("member"))
        .unwrap_or_else(|error| panic!("final identity should stage: {error:?}"));

    assert_eq!(
        candidate.local_member_id(),
        MemberId::try_from_raw(u64::MAX).unwrap_or_else(|| panic!("nonzero member identity"))
    );
    assert_eq!(candidate.next_member_id_after_install(), None);
    assert_eq!(catalog.next_member_id, MemberId::try_from_raw(u64::MAX));
}
