//! Sync terminal interpretation at the exact original absolute deadline.

use kafka_client_core::{ClassicGroupPhase, Moment};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_settlement_test::install_assignment_terminal,
    classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
    registry_test_support::stop_registry,
};

#[test]
fn late_success_terminalizes_once_without_install_or_replay() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    let deadline = Moment::from_tick(identity.deadline().core().tick());

    assert_eq!(
        registry.settle_one_classic_sync(deadline),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.classic.pending().is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.fault.is_none());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(_)
    ));

    assert_eq!(
        registry.settle_one_classic_sync(deadline),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("registered entry expected"))
            .execution
            .borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    stop_registry(&mut registry);
}
