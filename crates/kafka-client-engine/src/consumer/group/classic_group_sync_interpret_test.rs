//! Sync terminal interpretation at the exact original absolute deadline.

use kafka_client_core::{ClassicBrokerStage, ClassicGroupFatalReason, ClassicGroupPhase, Moment};

use crate::driver::classic_group::install_sync_broker_rejection_terminal;

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_settlement_test::{install_assignment_terminal, sync_key},
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

#[test]
fn load_in_progress_arms_the_exact_retained_coordinator_rejoin() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_sync_rejection(&mut registry, identity, 14);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.rejoin.schedule().is_some());
    assert!(entry.fault.is_none());

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}

#[test]
fn unknown_sync_code_becomes_the_exact_core_fatal() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_sync_rejection(&mut registry, identity, 1_235);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let fatal = entry
        .classic
        .machine()
        .fatal()
        .unwrap_or_else(|| panic!("fatal broker fact expected"));
    let ClassicGroupFatalReason::Broker { stage, error } = fatal.reason() else {
        panic!("broker fatal expected");
    };
    assert_eq!(stage, ClassicBrokerStage::Sync);
    assert_eq!(error.code(), 1_235);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Fatal);
    assert!(entry.rejoin.is_dormant());

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(4)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}

fn install_sync_rejection(
    registry: &mut super::registry::GroupConsumerRegistry,
    identity: super::classic_group_sync::ClassicGroupSyncIdentity,
    error_code: i16,
) {
    install_sync_broker_rejection_terminal(
        registry
            .sync_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Sync calls expected")),
        sync_key(identity),
        error_code,
    );
}
