//! Registry-owned Join submission, capacity, and driver rejection scenarios.

use std::time::Duration;

use kafka_client_core::ClassicGroupPhase;

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, classic_group::TrackedJoinGroupCalls},
};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join_execution::ClassicGroupJoinSubmissionTurn,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn bounded_capacity_leaves_the_exact_prepared_join_local() {
    let (mut registry, group_id) = prepared_registry();
    registry.join_calls = Some(TrackedJoinGroupCalls::new(0));
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_join(&driver),
        Ok(ClassicGroupJoinSubmissionTurn::Blocked)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert!(entry.execution.prepared_join().is_some());

    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

#[test]
fn exact_driver_acceptance_moves_join_into_tracked_ownership() {
    let (mut registry, group_id) = prepared_registry();
    let mut driver = driver();

    assert_eq!(
        registry.submit_one_classic_join(&driver),
        Ok(ClassicGroupJoinSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinDriverOwned(call)
            if call.identity().group_id() == group_id
    ));

    shutdown_driver(&mut driver);
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    stop_registry(&mut registry);
}

#[test]
fn driver_rejection_restores_the_exact_prepared_join() {
    let (mut registry, group_id) = prepared_registry();
    let mut driver = driver();
    driver
        .close_admission()
        .unwrap_or_else(|error| panic!("driver close admission failed: {error}"));
    let _turn = driver
        .turn(Duration::ZERO)
        .unwrap_or_else(|error| panic!("driver close turn failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_join(&driver),
        Ok(ClassicGroupJoinSubmissionTurn::Blocked)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert!(entry.execution.prepared_join().is_some());

    shutdown_driver(&mut driver);
    stop_registry(&mut registry);
}

fn prepared_registry() -> (
    super::registry::GroupConsumerRegistry,
    kafka_client_core::GroupId,
) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    registry
        .try_begin_classic_cycle(group_id, capture)
        .unwrap_or_else(|error| panic!("cycle begin failed: {error:?}"));
    (registry, group_id)
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
