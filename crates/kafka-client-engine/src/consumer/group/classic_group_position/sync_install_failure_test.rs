//! Atomic Sync installation when position preparation fails post-core.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupPhase, Moment, TopicId};

use crate::protocol::consumer::NamedAssignmentPartition;

use super::{
    super::{
        classic_group_entry_fault::ClassicGroupEntryFault,
        classic_group_execution::ClassicGroupExecutionError,
        classic_group_join::ClassicGroupExecutionState,
        classic_group_sync_install::install_sync_assignment,
        classic_group_sync_settlement_test::install_assignment_terminal,
        classic_group_sync_submission_test::{make_sync_driver_owned, prepared_registry},
        registry_test_support::stop_registry,
        session_catalog::GroupSessionCatalogError,
    },
    ClassicGroupPositionPreparationError,
};

#[test]
fn position_preparation_fault_keeps_every_install_owner_uncommitted() {
    let (mut registry, group_id, identity) = prepared_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);

    let mut entry = registry
        .entries
        .pop()
        .unwrap_or_else(|| panic!("registered entry expected"));
    let mut calls = registry
        .sync_calls
        .take()
        .unwrap_or_else(|| panic!("Sync calls expected"));
    let terminal = calls
        .begin_sync_group_settlement(
            entry
                .execution
                .sync_driver_owner()
                .unwrap_or_else(|| panic!("driver-owned Sync expected"))
                .accepted(),
        )
        .unwrap_or_else(|error| panic!("Sync settlement: {error:?}"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders identity expected"));
    let topic = entry
        .catalog
        .topics_by_id
        .remove(&topic_id)
        .unwrap_or_else(|| panic!("orders reverse identity expected"));
    entry
        .catalog
        .topics_by_id
        .insert(TopicId::from_raw(99), topic);

    let result = install_sync_assignment(
        &mut entry,
        identity.cycle(),
        Moment::from_tick(3),
        terminal,
        vec![
            NamedAssignmentPartition::from_assignment_decode_parts_for_test(Arc::from("orders"), 0),
        ],
    );
    let Err(failure) = result else {
        panic!("position preparation must fail");
    };
    assert_eq!(
        failure.into_parts().0,
        ClassicGroupExecutionError::PositionPreparation
    );
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert!(entry.classic.machine().live_assignment().is_some());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    assert!(entry.position.is_dormant());
    let terminal = match entry.fault.take() {
        Some(ClassicGroupEntryFault::SyncPositionPreparation { terminal, error }) => {
            assert_eq!(
                error,
                ClassicGroupPositionPreparationError::UnknownTopic(
                    GroupSessionCatalogError::UnknownTopic(topic_id)
                )
            );
            terminal
        }
        _ => panic!("exact position preparation fault expected"),
    };

    calls
        .restore_sync_group_settlement(terminal)
        .unwrap_or_else(|_failure| panic!("Sync terminal restoration failed"));
    let mut recovery = calls.recover_sync_groups_after_driver_shutdown();
    let recovered = recovery
        .take_settled()
        .unwrap_or_else(|| panic!("settled Sync recovery expected"));
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::SyncDriverOwned(owner) = state else {
        panic!("driver-owned Sync state expected");
    };
    let (_identity, accepted) = owner.into_parts();
    recovered
        .reconcile_sync_group_after_driver_shutdown(accepted)
        .unwrap_or_else(|_failure| panic!("exact Sync recovery receipt must reconcile"));
    drop(entry);
    stop_registry(&mut registry);
}
