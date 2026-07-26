//! Rediscovery terminal permission and terminal-failure gate scenarios.

use kafka_client_core::GroupId;

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationTerminalFailure,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn applied_and_stale_terminals_are_the_only_fresh_join_permissions() {
    for permission in [
        ClassicCoordinatorInvalidationPermission::Applied,
        ClassicCoordinatorInvalidationPermission::IgnoredStale,
    ] {
        let (mut registry, group_id) = invalidating_registry();
        registry
            .apply_classic_coordinator_invalidation_terminal(group_id, Ok(permission))
            .unwrap_or_else(|error| panic!("terminal permission failed: {error:?}"));
        assert!(
            !registry
                .entry(group_id)
                .unwrap_or_else(|| panic!("entry expected"))
                .rediscovery
                .blocks_join()
        );
        stop_registry(&mut registry);
    }
}

#[test]
fn capacity_terminal_cannot_claim_that_the_consumed_token_survived() {
    let (mut registry, group_id) = invalidating_registry();

    assert_eq!(
        registry.apply_classic_coordinator_invalidation_terminal(
            group_id,
            Err(ClassicCoordinatorInvalidationTerminalFailure::CapacityReached),
        ),
        Err(ClassicGroupExecutionError::CoordinatorInvalidationTerminal)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.rediscovery.blocks_join());
    assert!(matches!(
        &entry.fault,
        Some(ClassicGroupEntryFault::CoordinatorInvalidationTerminal(
            ClassicCoordinatorInvalidationTerminalFailure::CapacityReached
        ))
    ));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    drop(entry.fault.take());
    entry.rediscovery.clear_rediscovery_after_driver_shutdown();
    stop_registry(&mut registry);
}

#[test]
fn a_foreign_group_terminal_cannot_open_another_groups_gate() {
    let (mut registry, group_id) = invalidating_registry();
    let foreign = GroupId::try_from_raw(group_id.get() + 1)
        .unwrap_or_else(|| panic!("foreign group identity expected"));

    assert_eq!(
        registry.apply_classic_coordinator_invalidation_terminal(
            foreign,
            Ok(ClassicCoordinatorInvalidationPermission::Applied),
        ),
        Err(ClassicGroupExecutionError::CallIdentityMismatch)
    );
    assert!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .rediscovery
            .blocks_join()
    );
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .rediscovery
        .clear_rediscovery_after_driver_shutdown();
    stop_registry(&mut registry);
}

pub(super) fn invalidating_registry() -> (super::registry::GroupConsumerRegistry, GroupId) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let rediscovery = &mut registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .rediscovery;
    rediscovery
        .prepare_rediscovery_install()
        .unwrap_or_else(|error| panic!("rediscovery install failed: {error:?}"))
        .commit();
    rediscovery
        .confirm_rediscovery_transfer()
        .unwrap_or_else(|error| panic!("route transfer failed: {error:?}"));
    (registry, group_id)
}
