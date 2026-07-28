//! Exact completed-position transfer, restoration, and confirmation fencing.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, GroupPositionBatch,
    GroupPositionFence, GroupPositionPartitionFact, Moment, NextFetchOffset, PartitionIndex,
};

use crate::{
    clock::OperationDeadline,
    consumer::group::{
        classic_group_position::{
            ClassicGroupPositionActivationError, ClassicGroupPositionExecutionState,
            ClassicGroupPositionPreparation, prepare_classic_group_position,
            test_support::completed_ready,
        },
        classic_group_test_support,
        registry_entry::GroupConsumerEntry,
    },
    driver::{
        GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey,
        GroupPositionOffsetFetchTestPartition, TrackedGroupPositionOffsetFetchCalls,
    },
};

use super::{
    activation::ClassicGroupFetchActivationFailureKind,
    position_transfer::{
        ClassicGroupFetchTransferError, ClassicGroupFetchTransferTurn, transfer_completed_position,
    },
};

#[test]
fn confirmation_pending_position_is_not_transferred() {
    let mut entry = stable_entry();
    prepare_confirmation_pending(&mut entry);
    let expected_fence = entry
        .position
        .settlement_fence()
        .unwrap_or_else(|| panic!("confirmation-pending position expected"));

    assert_eq!(
        transfer_completed_position(
            &entry.classic,
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Ok(ClassicGroupFetchTransferTurn::Idle)
    );

    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));
    assert_eq!(entry.position.settlement_fence(), Some(expected_fence));
    assert!(entry.fetch.activation().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);
}

#[test]
fn confirmed_position_activates_fetch_exactly_once() {
    let mut entry = stable_entry();
    let current_fence = current_fence(&entry);
    install_completed(&mut entry, current_fence, Moment::from_tick(41), 17);

    assert_eq!(
        transfer_completed_position(
            &entry.classic,
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Ok(ClassicGroupFetchTransferTurn::Activated)
    );
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Dormant
    ));
    let installed_epoch = entry.fetch.machine_assignment_epoch();
    let effect_count = entry.fetch.effect_count_for_test();
    assert_eq!(
        entry
            .fetch
            .activation()
            .unwrap_or_else(|| panic!("Fetch activation expected"))
            .binding()
            .position_fence(),
        current_fence
    );

    assert_eq!(
        transfer_completed_position(
            &entry.classic,
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Ok(ClassicGroupFetchTransferTurn::Idle)
    );
    assert_eq!(entry.fetch.machine_assignment_epoch(), installed_epoch);
    assert_eq!(entry.fetch.effect_count_for_test(), effect_count);
}

#[test]
fn stale_completed_fence_restores_the_exact_position_owner() {
    let mut entry = stable_entry();
    let current_fence = current_fence(&entry);
    let stale_fence = GroupPositionFence::new(
        current_fence.group_id(),
        current_fence.membership_cycle(),
        current_fence.member_id(),
        AssignmentGeneration::try_from_raw(
            current_fence
                .assignment_generation()
                .get()
                .checked_add(1)
                .unwrap_or_else(|| panic!("next assignment generation")),
        )
        .unwrap_or_else(|| panic!("nonzero assignment generation")),
    );
    let observed_at = Moment::from_tick(73);
    install_completed(&mut entry, stale_fence, observed_at, 29);

    assert_eq!(
        transfer_completed_position(
            &entry.classic,
            &entry.catalog,
            &mut entry.position,
            &mut entry.fetch,
        ),
        Err(ClassicGroupFetchTransferError::Returned(
            ClassicGroupFetchActivationFailureKind::Position(
                ClassicGroupPositionActivationError::FenceMismatch {
                    completed: stale_fence,
                    current: current_fence,
                },
            ),
        ))
    );

    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Complete(completed)
            if completed.fence() == stale_fence
                && completed.observed_at() == observed_at
    ));
    assert!(entry.fetch.activation().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);
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

fn install_completed(
    entry: &mut GroupConsumerEntry,
    fence: GroupPositionFence,
    observed_at: Moment,
    next_offset: i64,
) {
    let partition = entry
        .catalog
        .live_assignment()
        .and_then(|assignment| assignment.partitions().first())
        .copied()
        .unwrap_or_else(|| panic!("assigned partition"));
    let completed = completed_ready(
        fence,
        observed_at,
        GroupPositionBatch::new(
            0,
            vec![GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(next_offset)
                    .unwrap_or_else(|| panic!("next Fetch offset")),
            )],
        ),
    );
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(completed));
}

fn prepare_confirmation_pending(entry: &mut GroupConsumerEntry) {
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active membership cycle"));
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(1_000));
    let preparation = prepare_classic_group_position(
        &entry.catalog,
        cycle,
        entry
            .catalog
            .live_assignment()
            .unwrap_or_else(|| panic!("live assignment")),
        deadline,
        Moment::from_tick(10),
    )
    .unwrap_or_else(|error| panic!("position preparation: {error:?}"));
    let ClassicGroupPositionPreparation::Prepared(prepared) = preparation else {
        panic!("nonempty assignment must prepare OffsetFetch");
    };
    let key = GroupPositionOffsetFetchKey::new(prepared.key().fence(), deadline);
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Prepared(prepared));
    let (handoff_key, request) = entry
        .position
        .begin_handoff()
        .unwrap_or_else(|error| panic!("position handoff: {error:?}"));
    assert_eq!(handoff_key.fence(), key.fence());
    drop(request);
    entry
        .position
        .confirm_driver_owned(GroupPositionOffsetFetchAccepted::from_fence_for_test(
            key.fence(),
        ))
        .unwrap_or_else(|_failure| panic!("position driver ownership"));

    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(1);
    calls.install_legacy_terminal_for_test(
        key,
        Some(7),
        0,
        0,
        &[(0, GroupPositionOffsetFetchTestPartition::Committed(17))],
    );
    let terminal = match entry.position.state() {
        ClassicGroupPositionExecutionState::DriverOwned(owner) => calls
            .begin_group_position_offset_fetch_settlement(owner.accepted())
            .unwrap_or_else(|error| panic!("terminal settlement: {error:?}")),
        _ => panic!("driver-owned position expected"),
    };
    entry
        .position
        .apply_raw_terminal(&terminal, Moment::from_tick(11))
        .unwrap_or_else(|failure| panic!("terminal application: {:?}", failure.error()));
}
