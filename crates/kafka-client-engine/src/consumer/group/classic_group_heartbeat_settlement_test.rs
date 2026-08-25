//! Heartbeat cadence, conservative failure, and raw-terminal restoration scenarios.

use kafka_client_core::{ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase, Moment};

use crate::driver::classic_group::{
    AcceptedClassicHeartbeatCall, ClassicHeartbeatCallKey, ClassicHeartbeatPoll,
    TrackedClassicHeartbeatCalls, install_heartbeat_success_terminal,
    install_heartbeat_transport_loss_terminal,
};

use super::{
    classic_group_assignment::retire_and_revoke_classic_group_assignment,
    classic_group_heartbeat::{
        ClassicHeartbeatDriverOwner, ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor,
        PreparedClassicHeartbeat,
    },
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    classic_group_heartbeat_settlement::ClassicHeartbeatSettlementTurn,
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry, stop_registry,
    },
};

#[test]
fn success_retains_broker_throttle_in_the_next_exact_cadence() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_success_terminal(heartbeat_calls(&mut registry), key, 1_500);
    let now = Moment::from_tick(key.deadline().core().tick() - 1);

    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    let schedule = match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::ConfirmationPending {
            successor: ClassicHeartbeatSuccessor::Waiting(schedule),
            ..
        } => *schedule,
        _ => panic!("next schedule must await exact route confirmation"),
    };
    assert_eq!(schedule.due().tick(), now.tick() + 1_500_000_000);
    assert_eq!(schedule.attempt().cycle(), key.attempt().cycle());
    assert_eq!(
        schedule.attempt().assignment_generation(),
        key.attempt().assignment_generation()
    );
    assert_eq!(
        schedule.attempt().sequence().get(),
        key.attempt().sequence().get() + 1
    );

    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .heartbeat
            .state(),
        ClassicHeartbeatExecutionState::Waiting(actual) if *actual == schedule
    ));
    stop_registry(&mut registry);
}

#[test]
fn coordinator_transport_close_without_route_evidence_rejoins_without_rediscovery_claim() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    make_driver_owned(&mut registry, group_id, key);
    install_heartbeat_transport_loss_terminal(heartbeat_calls(&mut registry), key);
    let now = Moment::from_tick(key.deadline().core().tick() - 1);

    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    let schedule = entry
        .rejoin
        .schedule()
        .unwrap_or_else(|| panic!("retained rejoin schedule expected"));
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    assert!(entry.catalog.live_assignment().is_some());
    assert!(!entry.revocation.is_dormant());
    assert!(!entry.rediscovery.blocks_join());

    assert_eq!(
        registry.settle_one_classic_heartbeat(now),
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    );
    assert!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .heartbeat
            .is_dormant()
    );

    stop_registry(&mut registry);
}

#[test]
fn core_fence_rejection_restores_the_exact_raw_terminal() {
    let (mut registry, group_id, key) = prepared_heartbeat();
    let (_first, next) = crate::driver::classic_group::heartbeat_attempts();
    let wrong = ClassicHeartbeatCallKey::new(group_id, next, key.deadline());
    make_driver_owned(&mut registry, group_id, wrong);
    install_heartbeat_success_terminal(heartbeat_calls(&mut registry), wrong, 0);
    let now = Moment::from_tick(key.deadline().core().tick() - 1);

    assert!(registry.settle_one_classic_heartbeat(now).is_err());
    assert_eq!(
        registry
            .heartbeat_calls
            .as_mut()
            .unwrap_or_else(|| panic!("Heartbeat calls expected"))
            .poll_classic_heartbeat(),
        Ok(ClassicHeartbeatPoll::TerminalReady { key: wrong })
    );
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("entry expected"))
            .heartbeat
            .state(),
        ClassicHeartbeatExecutionState::DriverOwned(owner)
            if owner.accepted().key() == wrong
    ));

    reconcile_fake_call(&mut registry, group_id);
    fail_live_attempt(&mut registry, group_id, key);
    stop_registry(&mut registry);
}

pub(super) fn prepared_heartbeat() -> (
    super::registry::GroupConsumerRegistry,
    kafka_client_core::GroupId,
    ClassicHeartbeatCallKey,
) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("entry expected"));
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
    }
    let schedule = match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::Waiting(schedule) => *schedule,
        _ => panic!("waiting Heartbeat expected"),
    };
    let clock = crate::clock::MonotonicClock::new();
    assert_eq!(
        registry.prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock,),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let key = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .map_or_else(
            || panic!("prepared Heartbeat expected"),
            PreparedClassicHeartbeat::key,
        );
    (registry, group_id, key)
}

pub(super) fn make_driver_owned(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    key: ClassicHeartbeatCallKey,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::Prepared(prepared) = state else {
        panic!("prepared Heartbeat expected");
    };
    drop(prepared);
    entry
        .heartbeat
        .set(ClassicHeartbeatExecutionState::DriverOwned(
            ClassicHeartbeatDriverOwner::new(AcceptedClassicHeartbeatCall::from_key_for_test(key)),
        ));
}

pub(super) fn heartbeat_calls(
    registry: &mut super::registry::GroupConsumerRegistry,
) -> &mut TrackedClassicHeartbeatCalls {
    registry
        .heartbeat_calls
        .as_mut()
        .unwrap_or_else(|| panic!("Heartbeat calls expected"))
}

fn reconcile_fake_call(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) {
    let calls = registry
        .heartbeat_calls
        .take()
        .unwrap_or_else(|| panic!("Heartbeat calls expected"));
    let mut recovery = calls.recover_classic_heartbeats_after_driver_shutdown();
    let recovered = recovery
        .take_settled()
        .unwrap_or_else(|| panic!("settled Heartbeat recovery expected"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned Heartbeat expected");
    };
    recovered
        .reconcile_classic_heartbeat_after_driver_shutdown(owner.into_accepted())
        .unwrap_or_else(|_failure| panic!("exact recovered Heartbeat must reconcile"));
    registry.heartbeat_calls =
        Some(crate::driver::classic_group::TrackedClassicHeartbeatCalls::new(8));
}

pub(super) fn fail_live_attempt(
    registry: &mut super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    key: ClassicHeartbeatCallKey,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::AssignmentLost {
            cycle: key.attempt().cycle(),
        })
        .unwrap_or_else(|error| panic!("failure transition rejected: {error}"));
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = transition.into_effects().next()
    else {
        panic!("Revoke expected");
    };
    retire_and_revoke_classic_group_assignment(
        &entry.classic,
        &mut entry.catalog,
        &mut entry.processing_lease,
        &mut entry.fetch,
        assignment,
        classic_generation,
    )
    .unwrap_or_else(|failure| panic!("revoke failed: {:?}", failure.kind));
}
