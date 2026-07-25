//! Registry-owned Sync normalization, install, confirmation, and restoration scenarios.

use kafka_client_core::{
    ClassicGroupPhase, GroupAssignmentPartition, LiveGroupAssignment, Moment, PartitionIndex,
};

use crate::driver::classic_group::{
    SyncGroupCallKey, SyncGroupPoll, install_malformed_sync_terminal,
    install_sync_assignment_terminal,
};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_submission_test::{
        make_sync_driver_owned, prepared_registry, recover_owned_sync_after_driver_shutdown,
    },
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
fn protocol_fault_restores_the_exact_raw_terminal_without_installing() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_malformed_terminal(&mut registry, identity);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Err(super::classic_group_execution::ClassicGroupExecutionError::SyncTerminal)
    );
    assert_uninstalled_restored(&mut registry, group_id, identity);

    recover_owned_sync_after_driver_shutdown(&mut registry, group_id);
    stop_registry(&mut registry);
}

#[test]
fn catalog_decode_fault_restores_the_exact_raw_terminal_without_installing() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "payments", &[0]);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Err(super::classic_group_execution::ClassicGroupExecutionError::SyncTerminal)
    );
    assert_uninstalled_restored(&mut registry, group_id, identity);

    recover_owned_sync_after_driver_shutdown(&mut registry, group_id);
    stop_registry(&mut registry);
}

fn assert_uninstalled_restored(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    identity: super::classic_group_sync::ClassicGroupSyncIdentity,
) {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Syncing);
    assert!(entry.classic.pending().is_some());
    assert!(entry.classic.machine().live_assignment().is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncDriverOwned(owner)
            if owner.identity() == identity
    ));
    assert_eq!(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected"))
            .poll_sync_group(),
        Ok(SyncGroupPoll::TerminalReady {
            key: sync_key(identity)
        })
    );
}

pub(super) fn install_assignment_terminal(
    registry: &mut super::registry::GroupConsumerRegistry,
    identity: super::classic_group_sync::ClassicGroupSyncIdentity,
    topic: &str,
    partitions: &[i32],
) {
    install_sync_assignment_terminal(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected")),
        sync_key(identity),
        topic,
        partitions,
    );
}

pub(super) fn install_malformed_terminal(
    registry: &mut super::registry::GroupConsumerRegistry,
    identity: super::classic_group_sync::ClassicGroupSyncIdentity,
) {
    install_malformed_sync_terminal(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected")),
        sync_key(identity),
    );
}

fn sync_key(identity: super::classic_group_sync::ClassicGroupSyncIdentity) -> SyncGroupCallKey {
    SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline())
}
