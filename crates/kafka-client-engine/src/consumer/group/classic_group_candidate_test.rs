//! Owned cycle-candidate staging, correlation, and rollback scenarios.

use std::sync::Arc;

use kafka_client_core::{GroupId, JoinedMemberSlot, MembershipCycle, TopicId};

use super::{classic_group_candidate::JoinedGroupMember, session_catalog::GroupSessionCatalog};

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
fn leader_candidate_ranks_member_spellings_and_stages_foreign_topics() {
    let catalog = catalog();
    let candidate = catalog
        .prepare_leader_cycle(
            MembershipCycle::initial(),
            Arc::from("a-local"),
            vec![
                JoinedGroupMember::new(slot(1), Arc::from("z-remote"), vec![Arc::from("payments")]),
                JoinedGroupMember::new(slot(2), Arc::from("a-local"), vec![Arc::from("orders")]),
            ],
        )
        .unwrap_or_else(|error| panic!("leader candidate failed: {error:?}"));

    assert_eq!(candidate.local_slot(), Some(slot(2)));
    assert_eq!(candidate.local_member_id().get(), 1);
    assert_eq!(
        candidate
            .member_spelling(slot(1))
            .map(std::convert::AsRef::as_ref),
        Some("z-remote")
    );
    assert_eq!(
        candidate
            .topic_name(&catalog, TopicId::from_raw(2))
            .map(std::convert::AsRef::as_ref),
        Some("payments")
    );
    let core = candidate
        .try_core_join_members()
        .unwrap_or_else(|error| panic!("core facts failed: {error:?}"));
    assert_eq!(core.members()[0].rank().get(), 1);
    assert_eq!(core.members()[0].slot(), slot(2));
    assert_eq!(core.members()[1].rank().get(), 2);
    assert_eq!(core.members()[1].slot(), slot(1));
    assert_eq!(core.members()[0].member_id().get(), 1);
    assert_eq!(core.members()[1].member_id().get(), 2);
    assert_eq!(catalog.topic_id("payments"), None);
    assert_eq!(
        catalog.next_member_id.map(kafka_client_core::MemberId::get),
        Some(1)
    );
    assert_eq!(catalog.next_topic_id.map(TopicId::get), Some(2));
}
