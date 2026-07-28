//! Deterministic bounded coalescing evidence for rebalance observation.

use std::sync::Arc;

use kafka_client_core::ClassicGroupPhase;

use crate::consumer::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerEvent,
};

use super::{
    classic_group_event::ClassicGroupEventStore,
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn unobserved_assignment_is_superseded_only_by_loss_of_that_epoch() {
    let mut events = confirmed(1);

    events.observe_retirement(Some(assignment(1)), 1, ClassicGroupPhase::Lost);

    assert_lost(events.take(), 1);
    assert_eq!(events.take(), None);
}

#[test]
fn prior_loss_and_new_assignment_remain_observable_in_order() {
    let mut events = confirmed(1);
    let _assigned = events.take();
    events.observe_retirement(Some(assignment(1)), 1, ClassicGroupPhase::Lost);
    events.stage_assignment(assignment(2));
    events.confirm_sync();

    assert_lost(events.take(), 1);
    assert_assigned(events.take(), 2);
    assert_eq!(events.take(), None);
}

#[test]
fn graceful_revocation_supersedes_assignment_without_duplicate_loss() {
    let mut events = confirmed(1);
    events.stage_graceful_revocation(Some(assignment(1)), 1);

    let Some(GroupConsumerEvent::PartitionsRevoked(revoked)) = events.take() else {
        panic!("expected revoked event");
    };
    assert_eq!(revoked.assignment_epoch(), 1);
    events.observe_retirement(Some(assignment(1)), 1, ClassicGroupPhase::WaitingToRejoin);
    assert_eq!(events.take(), None);
}

#[test]
fn consecutive_losses_coalesce_to_newest_cumulative_fence() {
    let mut events = confirmed(1);
    let _assigned = events.take();
    events.observe_retirement(Some(assignment(1)), 1, ClassicGroupPhase::Lost);
    events.stage_assignment(assignment(2));
    events.confirm_sync();
    events.observe_retirement(Some(assignment(2)), 2, ClassicGroupPhase::Lost);

    assert_lost(events.take(), 2);
    assert_eq!(events.take(), None);
}

#[test]
fn close_clears_observation_without_fabricating_loss() {
    let mut events = confirmed(1);

    events.observe_retirement(Some(assignment(1)), 1, ClassicGroupPhase::Closed);

    assert_eq!(events.take(), None);
}

#[test]
fn current_state_exists_only_after_sync_confirmation_and_survives_event_observation() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);

    assert_eq!(registry.group_state(group_id), Ok(None));
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered group expected"));
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
    }

    let state = registry
        .group_state(group_id)
        .unwrap_or_else(|error| panic!("state observation failed: {error:?}"))
        .unwrap_or_else(|| panic!("confirmed state expected"));
    assert_eq!(state.assignment().assignment_epoch(), 1);
    assert_eq!(state.assignment().partitions().len(), 1);
    assert_eq!(state.assignment().partitions()[0].topic(), "orders");
    assert_eq!(state.metadata().group(), "workers");
    assert_eq!(state.metadata().member(), "member-1");
    assert_eq!(state.metadata().generation_id(), 7);
    assert_eq!(state.metadata().assignment_epoch(), 1);
    assert_eq!(state.metadata().group_instance_id(), None);

    assert!(matches!(
        registry.take_event(group_id),
        Ok(Some(GroupConsumerEvent::PartitionsAssigned(_)))
    ));
    assert!(
        registry
            .group_state(group_id)
            .unwrap_or_else(|error| panic!("state observation failed: {error:?}"))
            .is_some()
    );
    stop_registry(&mut registry);
}

#[test]
fn confirmed_static_state_carries_the_configured_instance_identity() {
    let mut registry = started_registry();
    let group_id = registry
        .try_register_with_configuration(
            Arc::from("workers"),
            Some(Arc::from("instance-a")),
            vec![Arc::from("orders")],
            super::classic_group_test_support::timing(),
            super::classic_group_test_support::heartbeat_policy(),
            super::classic_group_test_support::rejoin_policy(),
            kafka_client_core::ReadIsolation::ReadUncommitted,
            super::registry_entry::default_classic_processing_lease_policy(),
        )
        .unwrap_or_else(|failure| panic!("static registration: {:?}", failure.kind));
    install_session(&mut registry, group_id);
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    entry.catalog.stage_installed_assignment_event();
    entry.catalog.confirm_sync_event();

    let state = registry
        .group_state(group_id)
        .unwrap_or_else(|error| panic!("state observation: {error:?}"))
        .unwrap_or_else(|| panic!("confirmed state"));
    assert_eq!(state.metadata().group_instance_id(), Some("instance-a"));
    stop_registry(&mut registry);
}

fn confirmed(epoch: u64) -> ClassicGroupEventStore {
    let mut events = ClassicGroupEventStore::new();
    events.stage_assignment(assignment(epoch));
    events.confirm_sync();
    events
}

fn assignment(epoch: u64) -> GroupConsumerAssignment {
    GroupConsumerAssignment::new(
        epoch,
        vec![GroupConsumerAssignmentPartition::new(
            Arc::from("orders"),
            0,
        )],
    )
}

fn assert_assigned(event: Option<GroupConsumerEvent>, epoch: u64) {
    let Some(GroupConsumerEvent::PartitionsAssigned(assignment)) = event else {
        panic!("expected assigned event");
    };
    assert_eq!(assignment.assignment_epoch(), epoch);
}

fn assert_lost(event: Option<GroupConsumerEvent>, epoch: u64) {
    let Some(GroupConsumerEvent::PartitionsLost(assignment)) = event else {
        panic!("expected lost event");
    };
    assert_eq!(assignment.assignment_epoch(), epoch);
}
