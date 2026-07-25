//! Sync exact two-phase route-confirmation and receipt-restoration scenarios.

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync_settlement_test::install_assignment_terminal,
    classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
    registry_test_support::stop_registry,
};
use crate::driver::classic_group::SyncGroupCallKey;

#[test]
fn exact_pending_route_confirmation_finishes_sync_ownership() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    let (entries, calls) = (&mut registry.entries, &mut registry.sync_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Sync calls expected"));
    let terminal = calls
        .begin_sync_group_settlement(
            entry
                .execution
                .sync_driver_owner()
                .unwrap_or_else(|| panic!("driver-owned Sync expected"))
                .accepted(),
        )
        .unwrap_or_else(|error| panic!("Sync settlement failed: {error:?}"));
    drop(terminal);
    entry
        .execution
        .stage_sync_confirmation()
        .unwrap_or_else(|error| panic!("Sync stage failed: {error:?}"));

    entry
        .execution
        .confirm_sync(calls)
        .unwrap_or_else(|error| panic!("Sync confirmation failed: {error:?}"));

    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    assert_eq!(calls.retained_sync_group_count(), 0);
    stop_registry(&mut registry);
}

#[test]
fn missing_pending_route_preserves_the_exact_sync_receipt() {
    let (mut registry, group_id, identity) = prepared_registry();
    let expected_key =
        SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);
    let (entries, calls) = (&mut registry.entries, &mut registry.sync_calls);
    let entry = entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let calls = calls
        .as_mut()
        .unwrap_or_else(|| panic!("Sync calls expected"));
    entry
        .execution
        .stage_sync_confirmation()
        .unwrap_or_else(|error| panic!("Sync stage failed: {error:?}"));

    assert_eq!(
        entry.execution.confirm_sync(calls),
        Err(ClassicGroupExecutionError::CallIdentityMismatch)
    );
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(owner)
            if owner.identity() == identity && owner.accepted().key() == expected_key
    ));

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}

#[test]
fn confirmation_staging_rejects_without_replacing_unrelated_state() {
    let (mut registry, group_id, _identity) = prepared_registry();
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));

    assert_eq!(
        entry.execution.stage_sync_confirmation(),
        Err(ClassicGroupExecutionError::HandoffMismatch)
    );
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(_)
    ));
    stop_registry(&mut registry);
}
