//! Modern member preparation and assignment commit scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupMemberEpoch, GroupAssignmentPartition, GroupId,
    LiveGroupAssignment, MembershipCycle, PartitionIndex,
};

use super::session_catalog::{GroupSessionCatalog, GroupSessionCatalogError};

#[test]
fn first_member_commits_without_inventing_a_classic_generation() {
    let mut catalog = catalog();
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate: {error:?}"));
    let member_id = candidate.member_id();
    let assignment = LiveGroupAssignment::try_new(
        group_id(),
        member_id,
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation")),
        vec![GroupAssignmentPartition::new(
            catalog
                .topic_id("orders")
                .unwrap_or_else(|| panic!("topic")),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"));
    let epoch = ConsumerGroupMemberEpoch::try_from_raw(3).unwrap_or_else(|| panic!("epoch"));
    catalog.commit_consumer_group_install(candidate, MembershipCycle::initial(), epoch, assignment);

    assert_eq!(catalog.current_member().map(Arc::as_ref), Some("member-a"));
    assert_eq!(catalog.current_member_id(), Some(member_id));
    assert_eq!(catalog.consumer_group_member_epoch(), Some(epoch));
    assert_eq!(catalog.classic_generation(), None);
    assert!(catalog.live_assignment().is_some());
}

#[test]
fn a_changed_broker_member_spelling_is_rejected_before_core_mutation() {
    let mut catalog = catalog();
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate: {error:?}"));
    let assignment = LiveGroupAssignment::try_new(
        group_id(),
        candidate.member_id(),
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation")),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"));
    catalog.commit_consumer_group_install(
        candidate,
        MembershipCycle::initial(),
        ConsumerGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("epoch")),
        assignment,
    );
    assert!(matches!(
        catalog.prepare_consumer_group_member(Arc::from("member-b")),
        Err(GroupSessionCatalogError::MemberMismatch)
    ));
}

#[test]
fn fenced_revoke_keeps_the_process_member_but_clears_epoch_and_assignment() {
    let mut catalog = catalog();
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate: {error:?}"));
    let member_id = candidate.member_id();
    let assignment = LiveGroupAssignment::try_new(
        group_id(),
        member_id,
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation")),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"));
    catalog.commit_consumer_group_install(
        candidate,
        MembershipCycle::initial(),
        ConsumerGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("epoch")),
        assignment,
    );
    let assignment = LiveGroupAssignment::try_new(
        group_id(),
        member_id,
        AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation")),
        Vec::new(),
    )
    .unwrap_or_else(|error| panic!("revoked assignment: {error:?}"));

    catalog.commit_consumer_group_fenced_revoke(assignment);

    assert_eq!(catalog.current_member_id(), Some(member_id));
    assert_eq!(catalog.current_member().map(Arc::as_ref), Some("member-a"));
    assert_eq!(catalog.consumer_group_member_epoch(), None);
    assert!(catalog.live_assignment().is_none());
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("rejoin candidate: {error:?}"));
    assert_eq!(candidate.member_id(), member_id);
}

#[test]
fn reconciliation_advances_epoch_then_replaces_only_the_retired_assignment() {
    let mut catalog = catalog();
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate: {error:?}"));
    let member_id = candidate.member_id();
    let first = assignment(&catalog, member_id, 1, 0);
    let first_epoch =
        ConsumerGroupMemberEpoch::try_from_raw(1).unwrap_or_else(|| panic!("first epoch"));
    catalog.commit_consumer_group_install(
        candidate,
        MembershipCycle::initial(),
        first_epoch,
        first,
    );
    let old = assignment(&catalog, member_id, 1, 0);
    let candidate = catalog
        .prepare_consumer_group_member(Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("reconciliation candidate: {error:?}"));
    let next_epoch =
        ConsumerGroupMemberEpoch::try_from_raw(2).unwrap_or_else(|| panic!("next epoch"));

    catalog.commit_consumer_group_reconciliation_epoch(&candidate, next_epoch);

    assert_eq!(catalog.consumer_group_member_epoch(), Some(next_epoch));
    assert_eq!(catalog.live_assignment(), Some(&old));
    catalog.commit_consumer_group_reconciliation_revoke(old);
    assert_eq!(catalog.current_member_id(), Some(member_id));
    assert_eq!(catalog.consumer_group_member_epoch(), Some(next_epoch));
    assert!(catalog.live_assignment().is_none());

    let target = assignment(&catalog, member_id, 2, 1);
    catalog.commit_consumer_group_reconciliation_install(
        candidate,
        MembershipCycle::initial()
            .checked_next()
            .unwrap_or_else(|| panic!("next cycle")),
        next_epoch,
        target,
    );
    let installed = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("target assignment"));
    assert_eq!(installed.assignment_generation().get(), 2);
    assert_eq!(installed.partitions()[0].partition().get(), 1);
    assert_eq!(catalog.consumer_group_member_epoch(), Some(next_epoch));
}

fn assignment(
    catalog: &GroupSessionCatalog,
    member_id: kafka_client_core::MemberId,
    generation: u64,
    partition: u32,
) -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        group_id(),
        member_id,
        AssignmentGeneration::try_from_raw(generation).unwrap_or_else(|| panic!("generation")),
        vec![GroupAssignmentPartition::new(
            catalog
                .topic_id("orders")
                .unwrap_or_else(|| panic!("topic")),
            PartitionIndex::from_raw(partition),
        )],
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"))
}

fn catalog() -> GroupSessionCatalog {
    GroupSessionCatalog::try_new(group_id(), Arc::from("workers"), &[Arc::from("orders")])
        .unwrap_or_else(|error| panic!("catalog: {error:?}"))
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
