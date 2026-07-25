//! Bounded membership timeout, close, and driver-ownership scheduling scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::clock::MonotonicClock;

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_execution::ClassicGroupExecutionError,
    registry_membership::GroupConsumerMembershipTurn,
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn one_due_local_cycle_expires_per_membership_turn() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    registry
        .try_begin_classic_cycle(first, capture)
        .unwrap_or_else(|error| panic!("first begin failed: {error:?}"));
    registry
        .try_begin_classic_cycle(second, capture)
        .unwrap_or_else(|error| panic!("second begin failed: {error:?}"));
    let due = Moment::from_tick(capture.deadline().tick());

    assert_eq!(
        registry.turn_membership(due),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(
        registry.turn_membership(due),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn closing_entry_revokes_its_exact_catalog_assignment() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));

    assert_eq!(registry.membership_unsettled(), 1);
    assert_eq!(
        registry.turn_membership(Moment::from_tick(1)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Closed);
    assert!(entry.catalog.live_assignment().is_none());
    assert_eq!(registry.membership_unsettled(), 0);
    stop_registry(&mut registry);
}

#[test]
fn driver_owned_join_blocks_close_without_local_deadline_mutation() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    registry
        .try_begin_classic_cycle(group_id, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    let handoff = entry
        .execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let tracking = entry
        .execution
        .confirm_join_driver_owned(handoff.into_driver_acceptance())
        .unwrap_or_else(|(error, _acceptance)| panic!("driver confirmation failed: {error:?}"));
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));

    assert_eq!(
        registry.turn_membership(Moment::from_tick(capture.deadline().tick())),
        Ok(GroupConsumerMembershipTurn::Blocked)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert_eq!(entry.execution.next_deadline(), None);
    assert_eq!(registry.membership_unsettled(), 1);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered entry expected"));
    entry
        .execution
        .recover_join_after_driver_shutdown(tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("driver recovery failed: {error:?}"));
    stop_registry(&mut registry);
}

#[test]
fn close_mismatch_retains_the_exact_revoke_effect_in_the_execution_owner() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    install_session(&mut registry, first);
    install_session(&mut registry, second);
    let (first_entry, second_entry) = registry.entries.split_at_mut(1);
    let first_entry = &mut first_entry[0];
    let second_entry = &mut second_entry[0];

    assert_eq!(
        first_entry
            .execution
            .close_if_local(&mut first_entry.classic, &mut second_entry.catalog,),
        Err(ClassicGroupExecutionError::Assignment(
            ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch,
        ))
    );

    let (assignment, generation, kind) = first_entry
        .execution
        .close_fault()
        .unwrap_or_else(|| panic!("exact close fault must remain retained"));
    assert_eq!(assignment.group_id(), first);
    assert_eq!(generation.get(), 7);
    assert_eq!(
        kind,
        ClassicGroupAssignmentPreparationFailureKind::AssignmentMismatch
    );
    assert!(first_entry.catalog.live_assignment().is_some());
    assert!(second_entry.catalog.live_assignment().is_some());
    first_entry
        .execution
        .retry_close_fault(&first_entry.classic, &mut first_entry.catalog)
        .unwrap_or_else(|error| panic!("retained revoke retry failed: {error:?}"));
    assert!(first_entry.catalog.live_assignment().is_none());
    assert!(second_entry.catalog.live_assignment().is_some());
    stop_registry(&mut registry);
}
