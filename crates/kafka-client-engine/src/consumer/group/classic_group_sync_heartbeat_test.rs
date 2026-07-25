//! Atomic Sync installation and conservative late-Sync heartbeat scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicGeneration, ClassicGroupInput, ClassicGroupPhase, Deadline, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync::ClassicGroupSyncIdentity,
    classic_group_sync_settlement::ClassicGroupSyncSettlementTurn,
    classic_group_sync_settlement_test::install_assignment_terminal,
    classic_group_sync_submission_test::{
        make_sync_driver_owned, prepared_registry, recover_owned_sync_after_driver_shutdown,
    },
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn occupied_heartbeat_rejects_before_core_or_catalog_install() {
    let mut schedule_registry = started_registry();
    let schedule_group = register(&mut schedule_registry, "schedule-source");
    install_session(&mut schedule_registry, schedule_group);
    let schedule = waiting_schedule(&schedule_registry, schedule_group);

    let (mut registry, group_id, identity) = prepared_registry();
    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .set(ClassicHeartbeatExecutionState::Waiting(schedule));
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);

    assert!(
        registry
            .settle_one_classic_sync(Moment::from_tick(3))
            .is_err()
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Syncing);
    assert!(entry.classic.machine().live_assignment().is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(matches!(
        entry.heartbeat.state(),
        ClassicHeartbeatExecutionState::Waiting(actual) if *actual == schedule
    ));

    registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .set(ClassicHeartbeatExecutionState::Dormant);
    recover_owned_sync_after_driver_shutdown(&mut registry, group_id);
    stop_registry(&mut registry);
    stop_registry(&mut schedule_registry);
}

#[test]
fn sync_after_join_anchored_liveness_confirms_without_installing() {
    let (mut registry, group_id, identity, liveness) = late_sync_registry();
    make_sync_driver_owned(&mut registry, group_id, identity);
    install_assignment_terminal(&mut registry, identity, "orders", &[0]);

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(liveness)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.classic.machine().live_assignment().is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncConfirmationPending(_)
    ));

    assert_eq!(
        registry.settle_one_classic_sync(Moment::from_tick(liveness)),
        Ok(ClassicGroupSyncSettlementTurn::Progress)
    );
    stop_registry(&mut registry);
}

fn late_sync_registry() -> (
    super::registry::GroupConsumerRegistry,
    kafka_client_core::GroupId,
    ClassicGroupSyncIdentity,
    u64,
) {
    const LATE_SETTLEMENT_TICK: u64 = u64::MAX;

    let mut registry = started_registry();
    let group_id = register(&mut registry, "late-workers");
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    entry
        .classic
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("Begin failed: {error}"));
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    let candidate = entry
        .catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate failed: {error:?}"));
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(u64::MAX));
    let generation =
        ClassicGeneration::try_from_raw(17).unwrap_or_else(|| panic!("generation expected"));
    let prepared = entry
        .classic
        .apply_follower_join(
            entry.catalog.group(),
            candidate,
            generation,
            Moment::from_tick(2),
            deadline,
        )
        .unwrap_or_else(|error| panic!("follower Join failed: {error:?}"));
    let identity = prepared.identity();
    entry
        .execution
        .set_execution_state(ClassicGroupExecutionState::PreparedSync(prepared));
    (registry, group_id, identity, LATE_SETTLEMENT_TICK)
}

fn waiting_schedule(
    registry: &super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) -> kafka_client_core::ClassicHeartbeatSchedule {
    match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::Waiting(schedule) => *schedule,
        _ => panic!("waiting Heartbeat expected"),
    }
}
