//! Ordinary hosted Heartbeat liveness-loss recovery evidence.

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::{
    clock::MonotonicClock,
    driver::classic_group::{
        install_heartbeat_success_terminal, install_heartbeat_transport_loss_terminal,
    },
};

use super::{
    classic_group_graceful_revocation::ClassicGroupRevocationTurn,
    classic_group_heartbeat::{ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor},
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    classic_group_heartbeat_settlement::ClassicHeartbeatSettlementTurn,
    classic_group_heartbeat_settlement_test::{
        heartbeat_calls, make_driver_owned, prepared_heartbeat,
    },
    classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntryState,
    registry_test_support::{deadline, install_session, register, started_registry, stop_registry},
};

#[test]
fn late_host_turn_revokes_before_arming_a_retained_rejoin() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);

    assert_eq!(
        registry.prepare_one_classic_heartbeat(
            Moment::from_tick(schedule.liveness_deadline().tick()),
            &MonotonicClock::new(),
        ),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.revocation.is_dormant());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.heartbeat.is_dormant());
    assert!(!entry.rediscovery.blocks_join());
    stop_registry(&mut registry);
}

#[test]
fn locally_blocked_attempt_expires_into_the_same_retained_rejoin() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let now = Moment::from_tick(schedule.due().tick());
    registry
        .prepare_one_classic_heartbeat(now, &MonotonicClock::new())
        .unwrap_or_else(|error| panic!("Heartbeat preparation failed: {error:?}"));
    let deadline = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .map_or_else(
            || panic!("prepared deadline expected"),
            |prepared| prepared.key().deadline().core(),
        );

    assert_eq!(
        registry.expire_one_prepared_heartbeat(Moment::from_tick(deadline.tick())),
        Ok(true)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(
        entry.rejoin.schedule(),
        entry.classic.machine().pending_rejoin()
    );
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.revocation.is_dormant());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.heartbeat.is_dormant());
    assert!(!entry.rediscovery.blocks_join());
    stop_registry(&mut registry);
}

#[test]
fn retained_route_loss_drains_revocation_before_preparing_a_fresh_rejoin() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_transport_loss_terminal(heartbeat_calls(&mut registry), key);
    let now = Moment::from_tick(key.deadline().core().tick() - 1);

    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    let schedule = registry
        .entry(group_id)
        .and_then(|entry| entry.rejoin.schedule())
        .unwrap_or_else(|| panic!("retained rejoin schedule expected"));
    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );

    let due = Moment::from_tick(schedule.due().tick());
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let revocation_deadline = entry
        .revocation
        .next_deadline()
        .unwrap_or_else(|| panic!("rebalance-bounded revocation deadline"));
    assert_eq!(revocation_deadline.tick(), now.tick() + 30_000_000_000);
    let assignment_epoch = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("retained assignment"))
        .assignment_generation()
        .get();
    assert_eq!(registry.membership_next_deadline(), None);
    assert_eq!(
        registry.prepare_one_classic_rejoin(due, &MonotonicClock::new()),
        Ok(ClassicGroupRejoinDueTurn::Idle)
    );
    registry
        .acknowledge_revocation(group_id, assignment_epoch, due)
        .unwrap_or_else(|error| panic!("revocation acknowledgment: {error:?}"));
    assert_eq!(
        registry.turn_graceful_revocation(due),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.revocation.is_dormant());
    assert!(entry.heartbeat.is_dormant());

    assert_eq!(
        registry.prepare_one_classic_rejoin(due, &MonotonicClock::new()),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert!(entry.execution.prepared_join().is_some());
    stop_registry(&mut registry);
}

#[test]
fn accepted_close_keeps_its_deadline_across_heartbeat_success_and_loss() {
    for succeeds in [true, false] {
        let (mut registry, group_id, key) = prepared_heartbeat();
        make_driver_owned(&mut registry, group_id, key);
        let now = Moment::from_tick(key.deadline().core().tick() - 1);
        let close_deadline = deadline(now.tick() + 1);
        let authority = registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .close_authority();
        let _completion = registry
            .close_group_explicit(group_id, close_deadline, &authority)
            .unwrap_or_else(|error| panic!("close admission failed: {error:?}"));
        if succeeds {
            install_heartbeat_success_terminal(heartbeat_calls(&mut registry), key, 0);
        } else {
            install_heartbeat_transport_loss_terminal(heartbeat_calls(&mut registry), key);
        }

        assert_eq!(
            registry.settle_one_classic_heartbeat(now),
            Ok(ClassicHeartbeatSettlementTurn::Progress)
        );
        let entry = registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"));
        assert_eq!(entry.state, GroupConsumerEntryState::Closing);
        assert_eq!(entry.leave.pending_deadline(), Some(close_deadline));
        assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Stable);
        assert!(entry.rejoin.is_dormant());
        assert!(!entry.rediscovery.blocks_join());
        assert!(entry.revocation.is_dormant());
        assert!(entry.revocation.next_deadline().is_none());
        assert!(entry.fetch.machine_assignment_epoch().is_some());
        assert!(matches!(
            entry.heartbeat.state(),
            ClassicHeartbeatExecutionState::ConfirmationPending {
                successor: ClassicHeartbeatSuccessor::Dormant,
                ..
            }
        ));

        assert_eq!(
            registry.settle_one_classic_heartbeat(now),
            Ok(ClassicHeartbeatSettlementTurn::Progress)
        );
        let entry = registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"));
        assert_eq!(entry.leave.pending_deadline(), Some(close_deadline));
        assert!(entry.heartbeat.is_dormant());
        assert!(entry.rejoin.is_dormant());
        assert!(entry.revocation.is_dormant());

        registry
            .recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("registry recovery failed: {error}"));
        let join = registry
            .finish_shutdown()
            .unwrap_or_else(|error| panic!("finish shutdown failed: {error}"));
        join.join_off_notifier()
            .unwrap_or_else(|error| panic!("notifier join failed: {error}"));
    }
}

fn schedule(
    registry: &GroupConsumerRegistry,
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
