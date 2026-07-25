//! Registry-owned follower Sync submission and rejection scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ClassicGeneration, ClassicGroupInput, ClassicGroupPhase, GroupId, Moment};

use crate::{
    EngineConfig,
    driver::{
        DriverOwner,
        classic_group::{SyncGroupCallKey, TrackedSyncGroupCalls},
    },
};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync::ClassicGroupSyncIdentity,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry::GROUP_CONSUMER_CAPACITY,
    registry_test_support::{deadline, register, started_registry, stop_registry},
};

#[test]
fn capacity_and_closing_leave_the_exact_prepared_sync_local() {
    let (mut capacity_registry, capacity_group, capacity_identity) = prepared_registry();
    capacity_registry.sync_calls = Some(TrackedSyncGroupCalls::new(0));
    let mut capacity_driver = driver();

    assert_eq!(
        capacity_registry.submit_one_classic_sync(&capacity_driver),
        Ok(ClassicGroupSyncSubmissionTurn::Blocked)
    );
    assert_eq!(
        prepared_identity(&capacity_registry, capacity_group),
        Some(capacity_identity)
    );
    shutdown_driver(&mut capacity_driver);
    stop_registry(&mut capacity_registry);

    let (mut closing_registry, closing_group, closing_identity) = prepared_registry();
    closing_registry
        .close_group(closing_group)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));
    let mut closing_driver = driver();

    assert_eq!(
        closing_registry.submit_one_classic_sync(&closing_driver),
        Ok(ClassicGroupSyncSubmissionTurn::Idle)
    );
    assert_eq!(
        prepared_identity(&closing_registry, closing_group),
        Some(closing_identity)
    );
    shutdown_driver(&mut closing_driver);
    stop_registry(&mut closing_registry);
}

#[test]
fn exact_driver_acceptance_moves_the_registry_entry_to_driver_ownership() {
    let (mut registry, group_id, identity) = prepared_registry();
    let mut driver = driver();
    let expected_key =
        SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());

    assert_eq!(
        registry.submit_one_classic_sync(&driver),
        Ok(ClassicGroupSyncSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncDriverOwned(owner)
            if owner.identity() == identity
                && owner.accepted().key() == expected_key
    ));

    shutdown_driver(&mut driver);
    recover_owned_sync_after_driver_shutdown(&mut registry, group_id);
    stop_registry(&mut registry);
}

#[test]
fn driver_admission_rejection_terminalizes_core_and_execution() {
    let (mut registry, group_id, _identity) = prepared_registry();
    let mut driver = driver();
    driver
        .close_admission()
        .unwrap_or_else(|error| panic!("driver close admission failed: {error}"));
    let _turn = driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("driver close turn failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_sync(&driver),
        Ok(ClassicGroupSyncSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
    assert!(entry.fault.is_none());

    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

pub(super) fn prepared_registry() -> (
    super::registry::GroupConsumerRegistry,
    GroupId,
    ClassicGroupSyncIdentity,
) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let operation_deadline = deadline(100);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let cycle = super::classic_group_test_support::begin(&mut entry.classic);
    let candidate = entry
        .catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    let prepared = entry
        .classic
        .apply_follower_join(
            entry.catalog.group(),
            candidate,
            generation(),
            Moment::from_tick(2),
            operation_deadline,
        )
        .unwrap_or_else(|error| panic!("follower Join failed: {error:?}"));
    let identity = prepared.identity();
    entry
        .execution
        .set_execution_state(ClassicGroupExecutionState::PreparedSync(prepared));
    (registry, group_id, identity)
}

pub(super) fn make_sync_driver_owned(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: GroupId,
    identity: ClassicGroupSyncIdentity,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let prepared = entry
        .execution
        .begin_sync_handoff()
        .unwrap_or_else(|error| panic!("Sync handoff failed: {error:?}"));
    drop(prepared);
    let key = crate::driver::classic_group::SyncGroupCallKey::new(
        identity.group_id(),
        identity.cycle(),
        identity.deadline(),
    );
    entry
        .execution
        .confirm_sync_driver_owned(
            identity,
            crate::driver::classic_group::AcceptedSyncGroupCall::from_key_for_test(key),
        )
        .unwrap_or_else(|_failure| panic!("Sync driver ownership failed"));
}

pub(super) fn recover_owned_sync_after_driver_shutdown(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: GroupId,
) {
    let calls = registry
        .sync_calls
        .take()
        .unwrap_or_else(|| panic!("Sync call owner expected"));
    let mut recovery = calls.recover_sync_groups_after_driver_shutdown();
    let recovered = recovery
        .pop_active()
        .or_else(|| recovery.take_settled())
        .or_else(|| recovery.take_pending())
        .or_else(|| recovery.take_completion())
        .unwrap_or_else(|| panic!("one recovered Sync owner expected"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let owner = match state {
        ClassicGroupExecutionState::SyncDriverOwned(owner)
        | ClassicGroupExecutionState::SyncConfirmationPending(owner) => owner,
        state => {
            entry.execution.set_execution_state(state);
            panic!("driver-owned Sync state expected");
        }
    };
    let (identity, accepted) = owner.into_parts();
    assert_eq!(recovered.key().group_id(), identity.group_id());
    assert_eq!(recovered.key().cycle(), identity.cycle());
    assert_eq!(recovered.key().deadline(), identity.deadline());
    if let Err(_failure) = recovered.reconcile_sync_group_after_driver_shutdown(accepted) {
        panic!("exact Sync recovery receipt must reconcile");
    }
    let transition = entry
        .classic
        .apply(ClassicGroupInput::SyncFailed {
            cycle: identity.cycle(),
        })
        .unwrap_or_else(|error| panic!("Sync recovery failure input failed: {error}"));
    assert!(transition.into_effects().next().is_none());
    assert!(recovery.is_empty());
    registry.sync_calls = Some(TrackedSyncGroupCalls::new(GROUP_CONSUMER_CAPACITY));
}

fn prepared_identity(
    registry: &super::registry::GroupConsumerRegistry,
    group_id: GroupId,
) -> Option<ClassicGroupSyncIdentity> {
    registry
        .entry(group_id)
        .and_then(|entry| entry.execution.prepared_sync())
        .map(super::classic_group_sync::PreparedClassicGroupSync::identity)
}

fn generation() -> ClassicGeneration {
    ClassicGeneration::try_from_raw(17).unwrap_or_else(|| panic!("classic generation"))
}

fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"))
}

fn shutdown_driver(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
}
