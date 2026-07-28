//! Semantic position failures remain position-owned rather than activating Fetch.

use std::sync::Arc;

use kafka_client_core::{
    GroupAssignmentPartition, GroupId, GroupPositionBatch, GroupPositionBootstrapTerminal,
    GroupPositionFence, GroupPositionPartitionFact, Moment, PartitionIndex,
};

use super::{
    super::{
        classic_group_position::{
            ClassicGroupPositionExecutionState, test_support::completed_ready,
        },
        classic_group_test_support,
        registry_entry::GroupConsumerEntry,
    },
    position_transfer::{ClassicGroupFetchTransferTurn, transfer_completed_position},
};

#[test]
fn classic_missing_offset_terminal_remains_owned_for_position_observation() {
    let mut entry = stable_entry();
    let fence = current_fence(&entry);
    install_missing(&mut entry, fence);

    assert_eq!(
        transfer_completed_position(
            &entry.classic,
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Ok(ClassicGroupFetchTransferTurn::Idle)
    );
    assert_missing_remains_fetch_inert(&entry);
}

fn stable_entry() -> GroupConsumerEntry {
    let group_id = GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group identity"));
    let mut entry = GroupConsumerEntry::try_new(
        group_id,
        &Arc::from("workers"),
        &[Arc::from("orders")],
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
    .unwrap_or_else(|error| panic!("group entry: {error:?}"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity"));
    let _heartbeat = classic_group_test_support::install_follower(
        &mut entry.catalog,
        &mut entry.classic,
        "member-1",
        7,
        vec![GroupAssignmentPartition::new(
            topic_id,
            PartitionIndex::from_raw(0),
        )],
    );
    entry
}

fn current_fence(entry: &GroupConsumerEntry) -> GroupPositionFence {
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("catalog assignment"));
    GroupPositionFence::new(
        assignment.group_id(),
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active membership cycle")),
        assignment.member_id(),
        assignment.assignment_generation(),
    )
}

fn install_missing(entry: &mut GroupConsumerEntry, fence: GroupPositionFence) {
    let partition = entry
        .catalog
        .live_assignment()
        .and_then(|assignment| assignment.partitions().first())
        .copied()
        .unwrap_or_else(|| panic!("assigned partition"));
    let completed = completed_ready(
        fence,
        Moment::from_tick(41),
        GroupPositionBatch::new(0, vec![GroupPositionPartitionFact::missing(partition)]),
    );
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(completed));
}

fn assert_missing_remains_fetch_inert(entry: &GroupConsumerEntry) {
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Complete(completed)
            if matches!(
                completed.terminal(),
                GroupPositionBootstrapTerminal::MissingOffsets(_)
            )
    ));
    assert!(entry.fetch.activation().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);
}
