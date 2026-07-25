//! Kafka wire-domain bounds at classic-group assignment installation.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicGroupInput, GroupAssignmentPartition, GroupId,
    LiveGroupAssignment, Moment, PartitionIndex, TopicId,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_owner::ClassicGroupOwner, classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

fn stable_effect(
    partition: PartitionIndex,
) -> (
    GroupSessionCatalog,
    ClassicGroupOwner,
    LiveGroupAssignment,
    ClassicGeneration,
) {
    let group_id = GroupId::try_from_raw(9).unwrap_or_else(|| panic!("nonzero group identity"));
    let catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
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
    let effect = owner
        .apply(ClassicGroupInput::SyncSucceeded {
            cycle,
            now: Moment::from_tick(3),
            partitions: vec![GroupAssignmentPartition::new(
                TopicId::from_raw(1),
                partition,
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
    } = effect
    else {
        panic!("Install expected");
    };
    (catalog, owner, assignment, classic_generation)
}

#[test]
fn signed_wire_maximum_installs_and_the_next_partition_is_rejected_atomically() {
    let maximum = PartitionIndex::from_raw(i32::MAX as u32);
    let (mut catalog, mut owner, assignment, generation) = stable_effect(maximum);
    owner
        .prepare_install(&mut catalog, assignment, generation)
        .unwrap_or_else(|failure| panic!("wire maximum rejected: {:?}", failure.kind))
        .commit();
    assert_eq!(
        catalog
            .live_assignment()
            .unwrap_or_else(|| panic!("installed assignment expected"))
            .partitions()[0]
            .partition(),
        maximum
    );

    let outside = PartitionIndex::from_raw((i32::MAX as u32) + 1);
    let (mut catalog, mut owner, assignment, generation) = stable_effect(outside);
    let failure = owner
        .prepare_install(&mut catalog, assignment, generation)
        .err()
        .unwrap_or_else(|| panic!("wire-domain overflow must reject"));
    assert_eq!(
        failure.kind,
        ClassicGroupAssignmentPreparationFailureKind::PartitionOutOfRange(outside)
    );
    assert_eq!(failure.assignment.partitions()[0].partition(), outside);
    assert!(owner.pending().is_some());
    assert!(catalog.live_assignment().is_none());
}
