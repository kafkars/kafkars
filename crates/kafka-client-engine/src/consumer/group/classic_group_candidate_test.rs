//! Owned cycle-candidate staging, correlation, and rollback scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicProtocol, GroupAssignmentPartition, GroupId, JoinedMemberSlot,
    MembershipCycle, PartitionIndex, TopicId,
};

use super::{
    classic_group_candidate::{
        ClassicGroupCycleCandidateError, JoinedGroupMember, JoinedOwnedPartition,
    },
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
fn leader_candidate_ranks_member_spellings_and_stages_foreign_topics() {
    let catalog = catalog();
    let candidate = catalog
        .prepare_leader_cycle(
            MembershipCycle::initial(),
            ClassicProtocol::Range,
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

#[test]
fn initial_cooperative_leader_echoes_empty_ownership_without_a_generation() {
    let catalog = catalog();
    let exact = catalog.prepare_leader_cycle(
        MembershipCycle::initial(),
        ClassicProtocol::CooperativeSticky,
        Arc::from("member-a"),
        vec![local_member(Vec::new(), None)],
    );
    assert!(exact.is_ok());

    let unexpected = catalog.prepare_leader_cycle(
        MembershipCycle::initial(),
        ClassicProtocol::CooperativeSticky,
        Arc::from("member-a"),
        vec![local_member(named_owned(&[0, 1]), Some(generation(7)))],
    );
    assert!(matches!(
        unexpected,
        Err(ClassicGroupCycleCandidateError::LocalOwnershipMismatch)
    ));
}

#[test]
fn current_cooperative_leader_rejects_a_different_local_generation() {
    let (catalog, cycle) = installed_cooperative_catalog();
    let exact = catalog.prepare_leader_cycle(
        cycle,
        ClassicProtocol::CooperativeSticky,
        Arc::from("member-a"),
        vec![local_member(named_owned(&[0, 1]), Some(generation(7)))],
    );
    assert!(exact.is_ok());

    let mismatch = catalog.prepare_leader_cycle(
        cycle,
        ClassicProtocol::CooperativeSticky,
        Arc::from("member-a"),
        vec![local_member(named_owned(&[0, 1]), Some(generation(8)))],
    );

    assert!(matches!(
        mismatch,
        Err(ClassicGroupCycleCandidateError::LocalOwnershipMismatch)
    ));
}

#[test]
fn current_cooperative_leader_rejects_different_local_partitions() {
    let (catalog, cycle) = installed_cooperative_catalog();
    let mismatch = catalog.prepare_leader_cycle(
        cycle,
        ClassicProtocol::CooperativeSticky,
        Arc::from("member-a"),
        vec![local_member(named_owned(&[0, 2]), Some(generation(7)))],
    );

    assert!(matches!(
        mismatch,
        Err(ClassicGroupCycleCandidateError::LocalOwnershipMismatch)
    ));
}

fn installed_cooperative_catalog() -> (GroupSessionCatalog, MembershipCycle) {
    let mut catalog = catalog();
    let mut owner = ClassicGroupOwner::new_with_protocol(
        group_id(),
        ClassicProtocol::CooperativeSticky,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let orders = catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders topic identity"));
    classic_group_test_support::install_follower(
        &mut catalog,
        &mut owner,
        "member-a",
        7,
        vec![
            GroupAssignmentPartition::new(orders, PartitionIndex::from_raw(0)),
            GroupAssignmentPartition::new(orders, PartitionIndex::from_raw(1)),
        ],
    );
    let cycle = MembershipCycle::initial()
        .checked_next()
        .unwrap_or_else(|| panic!("second membership cycle"));
    (catalog, cycle)
}

fn local_member(
    owned: Vec<JoinedOwnedPartition>,
    generation: Option<ClassicGeneration>,
) -> JoinedGroupMember {
    JoinedGroupMember::new_with_owned(
        slot(1),
        Arc::from("member-a"),
        vec![Arc::from("orders")],
        owned,
        generation,
    )
}

fn named_owned(partitions: &[i32]) -> Vec<JoinedOwnedPartition> {
    partitions
        .iter()
        .map(|partition| JoinedOwnedPartition::new(Arc::from("orders"), *partition))
        .collect()
}

fn generation(raw: i32) -> ClassicGeneration {
    ClassicGeneration::try_from_raw(raw).unwrap_or_else(|| panic!("nonnegative generation"))
}
