//! Registry cycle-selection and original-deadline admission scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, GroupId};

use crate::clock::MonotonicClock;

use super::{
    classic_group_join::PreparedClassicGroupJoin,
    registry_cycle::GroupConsumerCycleAdmissionError,
    registry_test_support::{register, started_registry, stop_registry},
};

#[test]
fn registered_group_owns_the_exact_captured_cycle_deadline() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));

    let cycle = registry
        .try_begin_classic_cycle(group_id, capture)
        .unwrap_or_else(|error| panic!("cycle admission failed: {error:?}"));
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group expected"));

    assert_eq!(entry.classic.machine().active_cycle(), Some(cycle));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert_eq!(entry.execution.next_deadline(), Some(capture.deadline()));
    assert_eq!(
        entry
            .execution
            .prepared_join()
            .map(PreparedClassicGroupJoin::deadline),
        Some(capture.operation_deadline())
    );
    stop_registry(&mut registry);
}

#[test]
fn unknown_and_closed_groups_reject_without_starting_another_cycle() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    let unknown =
        GroupId::try_from_raw(99).unwrap_or_else(|| panic!("nonzero unknown group identity"));

    assert_eq!(
        registry.try_begin_classic_cycle(unknown, capture),
        Err(GroupConsumerCycleAdmissionError::UnknownGroup)
    );
    registry.close_admission();
    assert_eq!(
        registry.try_begin_classic_cycle(group_id, capture),
        Err(GroupConsumerCycleAdmissionError::RegistryClosed)
    );
    stop_registry(&mut registry);
}
