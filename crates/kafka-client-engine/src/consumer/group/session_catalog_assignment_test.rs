//! Exact catalog installation and revocation of core-owned assignments.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, GroupAssignmentPartition, GroupId,
    Moment, PartitionIndex, TopicId,
};

use super::{
    classic_group_owner::ClassicGroupOwner, classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

fn owners() -> (GroupSessionCatalog, ClassicGroupOwner) {
    let group_id = GroupId::try_from_raw(9).unwrap_or_else(|| panic!("nonzero group identity"));
    let catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    (
        catalog,
        ClassicGroupOwner::new(
            group_id,
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
        ),
    )
}

#[test]
fn install_moves_the_exact_core_assignment_and_advances_staged_identities() {
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

    assert!(catalog.live_assignment().is_none());
    assert_eq!(catalog.next_member_id, member_cursor);

    let install = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: vec![GroupAssignmentPartition::new(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(2),
            )],
        })
        .unwrap_or_else(|error| panic!("Sync failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Install expected"));
    let ClassicGroupEffect::Install {
        assignment,
        classic_generation,
        heartbeat: _heartbeat,
    } = install
    else {
        panic!("Install expected");
    };
    let assignment_storage = assignment.partitions().as_ptr();
    owner
        .prepare_install(&mut catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("install preparation failed: {:?}", failure.kind))
        .commit();

    let current = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("installed assignment expected"));
    assert_eq!(current.member_id(), member_id);
    assert_eq!(current.partitions().as_ptr(), assignment_storage);
    assert_eq!(current.partitions()[0].partition().get(), 2);
    assert_eq!(
        catalog.current_member().map(std::convert::AsRef::as_ref),
        Some("member-a")
    );
    assert_eq!(catalog.classic_generation(), Some(7));
    assert_eq!(
        catalog.next_member_id.map(kafka_client_core::MemberId::get),
        Some(2)
    );
}

#[test]
fn revoke_clears_only_after_core_assignment_loss() {
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
    let revoke = owner
        .apply(ClassicGroupInput::AssignmentLost { cycle })
        .unwrap_or_else(|error| panic!("assignment loss failed: {error}"))
        .into_effects()
        .next()
        .unwrap_or_else(|| panic!("Revoke expected"));
    let ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    } = revoke
    else {
        panic!("Revoke expected");
    };
    assert!(catalog.live_assignment().is_some());
    owner
        .prepare_revoke(&mut catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| panic!("revoke preparation failed: {:?}", failure.kind))
        .commit();
    assert!(catalog.live_assignment().is_none());
    assert!(catalog.current_member().is_none());
}
