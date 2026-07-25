//! Registry-wide Sync reconciliation across the post-driver shutdown boundary.

use kafka_client_core::{ClassicGroupErrorKind, ClassicGroupInput, ClassicGroupPhase, Moment};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_settlement_test::install_assignment_terminal,
    classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
    registry_test_support::stop_registry,
};

#[test]
fn sync_driver_owned_shutdown_applies_the_core_failure_once() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.classic.pending().is_none());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    stop_registry(&mut registry);
}

#[test]
fn sync_confirmation_shutdown_does_not_reapply_the_core_terminal() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(3)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert!(entry.catalog.live_assignment().is_some());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(_)
    ));

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert!(entry.catalog.live_assignment().is_some());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    stop_registry(&mut registry);
}

#[test]
fn core_rejection_retains_both_exact_sync_shutdown_owners() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::SyncFailed {
            cycle: identity.cycle(),
        })
        .unwrap_or_else(|error| panic!("precondition Sync failure failed: {error}"));
    assert!(transition.into_effects().next().is_none());

    assert_eq!(
        registry.recover_classic_calls_after_driver_shutdown(),
        Err(ClassicGroupExecutionError::Core(
            ClassicGroupErrorKind::InvalidPhase
        ))
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert!(matches!(
        &entry.fault,
        Some(ClassicGroupEntryFault::SyncRecoverySemantic(cycle))
            if *cycle == identity.cycle()
    ));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncDriverOwned(owner)
            if owner.identity() == identity
                && owner.accepted().key()
                    == registry
                        .sync_recovery_fault
                        .as_ref()
                        .unwrap_or_else(|| panic!("exact recovered Sync owner expected"))
                        .key()
    ));
    assert_eq!(registry.membership_unsettled(), 3);

    let recovered = registry
        .sync_recovery_fault
        .take()
        .unwrap_or_else(|| panic!("exact recovered Sync owner expected"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::SyncDriverOwned(owner) = state else {
        panic!("driver-owned Sync expected");
    };
    let (_identity, accepted) = owner.into_parts();
    recovered
        .reconcile_sync_group_after_driver_shutdown(accepted)
        .unwrap_or_else(|_failure| panic!("exact Sync recovery cleanup failed"));
    drop(entry.fault.take());
    stop_registry(&mut registry);
}
