//! Post-driver Heartbeat recovery and semantic replay-fencing scenarios.

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::driver::classic_group::{
    install_heartbeat_route_failure_terminal, install_heartbeat_success_terminal,
};

use super::{
    classic_group_heartbeat::{ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor},
    classic_group_heartbeat_settlement::ClassicHeartbeatSettlementTurn,
    classic_group_heartbeat_settlement_test::{
        heartbeat_calls, make_driver_owned, prepared_heartbeat,
    },
    registry_test_support::stop_registry,
};

#[test]
fn pre_semantic_terminal_recovers_as_conservative_assignment_loss() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_route_failure_terminal(heartbeat_calls(&mut registry), key);

    registry
        .recover_classic_heartbeats_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("Heartbeat recovery failed: {error:?}"));

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    registry.heartbeat_calls =
        Some(crate::driver::classic_group::TrackedClassicHeartbeatCalls::new(8));
    stop_registry(&mut registry);
}

#[test]
fn confirmation_pending_recovery_never_reapplies_the_success() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_success_terminal(heartbeat_calls(&mut registry), key, 0);
    let now = Moment::from_tick(key.deadline().core().tick() - 1);
    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    let expected = match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::ConfirmationPending {
            successor: ClassicHeartbeatSuccessor::Waiting(schedule),
            ..
        } => *schedule,
        _ => panic!("confirmation-pending schedule expected"),
    };

    registry
        .recover_classic_heartbeats_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("Heartbeat recovery failed: {error:?}"));

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
    assert!(entry.catalog.live_assignment().is_some());
    assert!(matches!(
        entry.heartbeat.state(),
        ClassicHeartbeatExecutionState::Waiting(schedule) if *schedule == expected
    ));
    registry.heartbeat_calls =
        Some(crate::driver::classic_group::TrackedClassicHeartbeatCalls::new(8));
    stop_registry(&mut registry);
}

#[test]
fn close_counts_and_drains_accepted_heartbeat_ownership_before_core_close() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_route_failure_terminal(heartbeat_calls(&mut registry), key);
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));

    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(0)),
        Ok(super::registry_membership::GroupConsumerMembershipTurn::Blocked)
    );
    assert_eq!(registry.membership_unsettled(), 3);

    registry
        .recover_classic_heartbeats_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("Heartbeat recovery failed: {error:?}"));
    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(u64::MAX)),
        Ok(super::registry_membership::GroupConsumerMembershipTurn::Progress)
    );
    assert_eq!(registry.membership_unsettled(), 0);
    registry.heartbeat_calls =
        Some(crate::driver::classic_group::TrackedClassicHeartbeatCalls::new(8));
    stop_registry(&mut registry);
}
