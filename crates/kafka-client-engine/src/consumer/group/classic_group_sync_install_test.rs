//! Atomic normalized Sync assignment installation before route confirmation.

use kafka_client_core::{
    ClassicGroupPhase, GroupAssignmentPartition, GroupPositionBootstrapTerminal,
    LiveGroupAssignment, Moment, PartitionIndex,
};

use crate::driver::classic_group::SyncGroupPoll;

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_position::ClassicGroupPositionExecutionState,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_settlement_test::{install_assignment_terminal, sync_key},
    classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
    registry_test_support::stop_registry,
};

#[test]
fn exact_sync_success_installs_before_route_confirmation() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[3]);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert_eq!(
        entry
            .catalog
            .live_assignment()
            .map(LiveGroupAssignment::partitions),
        Some(
            [GroupAssignmentPartition::new(
                topic_id,
                PartitionIndex::from_raw(3),
            )]
            .as_slice()
        )
    );
    assert!(!entry.heartbeat.is_dormant());
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Prepared(prepared)
            if prepared.key().fence().group_id() == group_id
                && prepared.key().fence().membership_cycle() == identity.cycle()
                && prepared.key().operation_deadline() == identity.deadline()
    ));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(owner)
            if owner.identity() == identity
    ));
    assert_eq!(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected"))
            .poll_sync_group(),
        Ok(SyncGroupPoll::ConfirmationPending {
            key: sync_key(identity)
        })
    );

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert!(entry.catalog.live_assignment().is_some());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Prepared(_)
    ));
    assert_eq!(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected"))
            .retained_sync_group_count(),
        0
    );
    stop_registry(&mut registry);
}

#[test]
fn empty_sync_assignment_completes_position_without_request_ownership() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[]);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let ClassicGroupPositionExecutionState::Complete(completed) = entry.position.state() else {
        panic!("empty assignment must not create RPC ownership");
    };
    assert_eq!(completed.fence().membership_cycle(), identity.cycle());
    assert!(matches!(
        completed.terminal(),
        GroupPositionBootstrapTerminal::Ready(batch) if batch.facts().is_empty()
    ));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(_)
    ));
    assert_eq!(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected"))
            .poll_sync_group(),
        Ok(SyncGroupPoll::ConfirmationPending {
            key: sync_key(identity)
        })
    );

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}
