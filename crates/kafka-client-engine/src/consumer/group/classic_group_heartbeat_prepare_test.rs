//! Heartbeat cadence, liveness, and absolute-deadline preparation scenarios.

use kafka_client_core::{ClassicGroupPhase, Moment};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn late_host_turn_revokes_instead_of_claiming_liveness() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = crate::clock::MonotonicClock::new();

    assert_eq!(
        registry.prepare_one_classic_heartbeat(
            Moment::from_tick(schedule.liveness_deadline().tick()),
            &clock,
        ),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    stop_registry(&mut registry);
}

#[test]
fn prepared_attempt_uses_the_core_deadline_mapped_by_the_shared_epoch() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = crate::clock::MonotonicClock::new();
    let now = Moment::from_tick(schedule.due().tick());

    assert_eq!(
        registry.prepare_one_classic_heartbeat(now, &clock),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let prepared = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .unwrap_or_else(|| panic!("prepared Heartbeat expected"));
    let expected_core = now
        .checked_deadline_after(
            super::classic_group_test_support::heartbeat_policy().attempt_timeout_ticks(),
        )
        .map_or_else(
            || panic!("test Heartbeat deadline must fit"),
            |deadline| deadline.min(schedule.liveness_deadline()),
        );
    let expected = clock
        .operation_deadline(expected_core)
        .unwrap_or_else(|error| panic!("exact deadline mapping failed: {error}"));

    assert_eq!(prepared.key().deadline(), expected);
    stop_registry(&mut registry);
}

#[test]
fn locally_blocked_prepared_attempt_expires_and_revokes() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = crate::clock::MonotonicClock::new();
    registry
        .prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock)
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
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    stop_registry(&mut registry);
}

fn schedule(
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
