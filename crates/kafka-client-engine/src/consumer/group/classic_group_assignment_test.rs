//! Core-authorized Install and Revoke catalog commit scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, Deadline,
    GroupAssignmentPartition, GroupId, Moment, PartitionIndex, TopicId,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_owner::ClassicGroupOwner, classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

fn owners() -> (GroupSessionCatalog, ClassicGroupOwner) {
    let group_id = GroupId::try_from_raw(9).unwrap_or_else(|| panic!("nonzero group identity"));
    let catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    (catalog, ClassicGroupOwner::new(group_id))
}

#[test]
fn unsubscribed_install_failure_retains_assignment_and_candidate() {
    let (mut catalog, mut owner) = owners();
    let cycle = classic_group_test_support::begin(&mut owner);
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate failed: {error:?}"));
    let member_id = candidate.local_member_id();
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));
    owner
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id,
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("classic generation")),
        })
        .unwrap_or_else(|error| panic!("Join failed: {error}"));
    let install = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: vec![GroupAssignmentPartition::new(
                TopicId::from_raw(99),
                PartitionIndex::from_raw(0),
            )],
        })
        .unwrap_or_else(|error| panic!("Sync failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Install expected"));
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
    } = install
    else {
        panic!("Install expected");
    };
    let failure = owner
        .prepare_install(&mut catalog, assignment, classic_generation)
        .err()
        .unwrap_or_else(|| panic!("unsubscribed topic must reject"));
    assert_eq!(
        failure.kind,
        ClassicGroupAssignmentPreparationFailureKind::UnsubscribedTopic
    );
    assert_eq!(
        failure.assignment.partitions()[0].topic_id(),
        TopicId::from_raw(99)
    );
    assert!(owner.pending().is_some());
    assert!(catalog.live_assignment().is_none());
}

#[test]
fn dropping_prepared_install_preserves_candidate_catalog_and_cursors() {
    let (mut catalog, mut owner) = owners();
    let cycle = classic_group_test_support::begin(&mut owner);
    let member_cursor = catalog.next_member_id;
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate failed: {error:?}"));
    let member_id = candidate.local_member_id();
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));
    owner
        .apply(ClassicGroupInput::JoinFollower {
            cycle,
            now: Moment::from_tick(2),
            member_id,
            generation: ClassicGeneration::try_from_raw(7)
                .unwrap_or_else(|| panic!("classic generation")),
        })
        .unwrap_or_else(|error| panic!("Join failed: {error}"));
    let effect = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: Vec::new(),
        })
        .unwrap_or_else(|error| panic!("Sync failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Install expected"));
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
    } = effect
    else {
        panic!("Install expected");
    };
    let prepared = owner
        .prepare_install(&mut catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("install preparation failed: {:?}", failure.kind));
    drop(prepared);

    assert!(owner.pending().is_some());
    assert!(catalog.live_assignment().is_none());
    assert_eq!(catalog.next_member_id, member_cursor);
}

#[test]
fn dropping_prepared_revoke_preserves_the_catalog_session() {
    let (mut catalog, mut owner) = owners();
    classic_group_test_support::install_follower(
        &mut catalog,
        &mut owner,
        "member-a",
        7,
        Vec::new(),
    );
    let cycle = owner
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("stable cycle expected"));
    let effect = owner
        .apply(ClassicGroupInput::AssignmentLost { cycle })
        .unwrap_or_else(|error| panic!("assignment loss failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Revoke expected"));
    let ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    } = effect
    else {
        panic!("Revoke expected");
    };
    let prepared = owner
        .prepare_revoke(&mut catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("revoke preparation failed: {:?}", failure.kind));
    drop(prepared);
    owner
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(4),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("next cycle failed after revoke drop: {error}"));

    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Joining);
    assert!(catalog.live_assignment().is_some());
    assert_eq!(
        catalog.current_member().map(std::convert::AsRef::as_ref),
        Some("member-a")
    );
    assert_eq!(catalog.classic_generation(), Some(7));
}

#[test]
fn foreign_lost_owner_cannot_authorize_another_groups_revoke() {
    let (mut catalog_a, mut owner_a) = owners();
    classic_group_test_support::install_follower(
        &mut catalog_a,
        &mut owner_a,
        "member-a",
        7,
        Vec::new(),
    );
    let cycle_a = owner_a
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("stable cycle expected"));
    owner_a
        .apply(ClassicGroupInput::AssignmentLost { cycle: cycle_a })
        .unwrap_or_else(|error| panic!("assignment loss failed: {error}"));

    let group_b = GroupId::try_from_raw(10).unwrap_or_else(|| panic!("nonzero group identity"));
    let mut catalog_b =
        GroupSessionCatalog::try_new(group_b, Arc::from("payments"), &[Arc::from("payments")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let mut owner_b = ClassicGroupOwner::new(group_b);
    classic_group_test_support::install_follower(
        &mut catalog_b,
        &mut owner_b,
        "member-b",
        11,
        Vec::new(),
    );
    let cycle_b = owner_b
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("stable cycle expected"));
    let effect = owner_b
        .apply(ClassicGroupInput::AssignmentLost { cycle: cycle_b })
        .unwrap_or_else(|error| panic!("assignment loss failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Revoke expected"));
    let ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    } = effect
    else {
        panic!("Revoke expected");
    };

    let failure = owner_a
        .prepare_revoke(&mut catalog_b, assignment, classic_generation)
        .err()
        .unwrap_or_else(|| panic!("foreign owner must reject"));
    assert_eq!(
        failure.kind,
        ClassicGroupAssignmentPreparationFailureKind::GroupMismatch
    );
    assert_eq!(failure.assignment.group_id(), group_b);
    assert!(catalog_b.live_assignment().is_some());
}
