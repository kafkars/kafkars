//! Bounded classic Heartbeat admission and handoff scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::driver::classic_group::TrackedClassicHeartbeatCalls;

use super::{
    classic_group_heartbeat::{ClassicHeartbeatExecutionState, PreparedClassicHeartbeat},
    classic_group_heartbeat_prepare::ClassicHeartbeatPreparationTurn,
    classic_group_heartbeat_submission::ClassicHeartbeatSubmissionTurn,
    registry_test_support::{install_session, register, started_registry},
};

#[test]
fn saturated_call_capacity_retains_the_exact_prepared_heartbeat() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::Waiting(schedule) => *schedule,
        _ => panic!("waiting heartbeat expected"),
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
            || panic!("prepared heartbeat expected"),
            PreparedClassicHeartbeat::key,
        );
    registry.heartbeat_calls = Some(TrackedClassicHeartbeatCalls::new(0));

    let mut driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
        "127.0.0.1:1".to_owned(),
    ]))
    .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    assert_eq!(
        registry.submit_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &driver),
        Ok(super::classic_group_heartbeat_submission::ClassicHeartbeatSubmissionTurn::Blocked)
    );
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.heartbeat.prepared())
            .map(PreparedClassicHeartbeat::key),
        Some(key)
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn mismatched_accepted_receipt_is_retained_in_the_entry_fault() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
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
    registry
        .prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock)
        .unwrap_or_else(|error| panic!("Heartbeat preparation failed: {error:?}"));
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
    let expected = prepared.key();
    drop(prepared);
    entry
        .heartbeat
        .set(ClassicHeartbeatExecutionState::Handoff(expected));
    let (_first, next) = crate::driver::classic_group::heartbeat_attempts();
    let supplied = crate::driver::classic_group::ClassicHeartbeatCallKey::new(
        group_id,
        next,
        expected.deadline(),
    );

    assert!(
        super::classic_group_heartbeat_submission::confirm_driver_owned(
            entry,
            expected,
            crate::driver::classic_group::AcceptedClassicHeartbeatCall::from_key_for_test(
                supplied,
            ),
        )
        .is_err()
    );
    let fault = entry
        .fault
        .take()
        .unwrap_or_else(|| panic!("acceptance fault expected"));
    match fault {
        super::classic_group_entry_fault::ClassicGroupEntryFault::HeartbeatAcceptance(failure) => {
            assert_eq!(failure.expected(), expected);
            assert_eq!(failure.accepted().key(), supplied);
        }
        _ => panic!("Heartbeat acceptance fault expected"),
    }
    super::classic_group_heartbeat_settlement_test::fail_live_attempt(
        &mut registry,
        group_id,
        expected,
    );
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn driver_admission_rejection_revokes_then_arms_a_retained_rejoin() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
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
    registry
        .prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock)
        .unwrap_or_else(|error| panic!("Heartbeat preparation failed: {error:?}"));
    let mut driver = crate::driver::DriverOwner::build(&crate::EngineConfig::new(vec![
        "127.0.0.1:1".to_owned(),
    ]))
    .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    driver
        .close_admission()
        .unwrap_or_else(|error| panic!("driver close admission failed: {error}"));
    let _turn = driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("driver close turn failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &driver),
        Ok(ClassicHeartbeatSubmissionTurn::Progress)
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
    assert!(entry.fault.is_none());
    assert_eq!(
        registry.heartbeat_calls.as_ref().map_or(
            usize::MAX,
            TrackedClassicHeartbeatCalls::retained_classic_heartbeat_count
        ),
        0
    );

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    super::registry_test_support::stop_registry(&mut registry);
}
