//! Confirmed cooperative split-authority state observation scenarios.

use kafka_client_core::{
    ClassicGroupInput, ClassicGroupPhase, GroupAssignmentPartition, GroupPositionFence, Moment,
};

use crate::consumer::GroupConsumerEvent;

use super::{
    registry_entry::GroupConsumerEntry,
    registry_event::{GroupConsumerStateError, observable_classic_position_fence},
};

#[cfg(test)]
mod fixture_test;

pub(super) use fixture_test::{
    activate_previous_fetch, defer_rejoin_during_reconciliation, prepared_reconciliation,
};

#[test]
fn deferred_rejoin_keeps_confirmed_reconciliation_state_observable() {
    let mut entry = prepared_reconciliation();
    entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"))
        .confirm_sync();
    let (expected, removed) = expected_previous_fence_and_removed(&entry);
    stage_removed_event(&mut entry, &removed);

    let schedule = defer_rejoin_during_reconciliation(&mut entry);

    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert_eq!(entry.classic.machine().pending_rejoin(), Some(schedule));
    assert_eq!(entry.rejoin.schedule(), Some(schedule));
    assert_eq!(
        observable_classic_position_fence(&entry),
        Ok(Some(expected))
    );
    assert!(matches!(
        entry.catalog.take_event(),
        Some(GroupConsumerEvent::PartitionsRevoked(_))
    ));
}

#[test]
fn confirmed_reconciliation_keeps_the_previous_fence_across_revocation_observation() {
    let mut entry = prepared_reconciliation();

    assert_eq!(observable_classic_position_fence(&entry), Ok(None));
    entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"))
        .confirm_sync();

    let (expected, removed) = expected_previous_fence_and_removed(&entry);
    assert_eq!(
        observable_classic_position_fence(&entry),
        Ok(Some(expected))
    );
    stage_removed_event(&mut entry, &removed);
    assert_eq!(
        observable_classic_position_fence(&entry),
        Ok(Some(expected))
    );

    let Some(GroupConsumerEvent::PartitionsRevoked(revoked)) = entry.catalog.take_event() else {
        panic!("cooperative removed-partition event");
    };
    assert_eq!(
        revoked.assignment_epoch(),
        expected.assignment_generation().get()
    );
    assert_eq!(revoked.partitions().len(), 1);
    assert_eq!(revoked.partitions()[0].topic(), "orders");
    assert_eq!(revoked.partitions()[0].partition(), 1);
    assert_eq!(
        observable_classic_position_fence(&entry),
        Ok(Some(expected))
    );
}

#[test]
fn post_core_phase_mismatch_remains_an_observation_fault() {
    let mut entry = prepared_reconciliation();
    let pending = entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"));
    pending.confirm_sync();
    let replacement = pending.reconciliation().replacement_assignment();
    let cycle = pending.reconciliation().replacement_cycle();
    let assignment_generation = replacement.assignment_generation();

    entry
        .classic
        .apply(ClassicGroupInput::ReconciliationApplied {
            cycle,
            assignment_generation,
            now: Moment::from_tick(20),
        })
        .unwrap_or_else(|error| panic!("advance core beyond retained split: {error}"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Joining);
    assert_eq!(
        observable_classic_position_fence(&entry),
        Err(GroupConsumerStateError::EntryFault)
    );
}

fn expected_previous_fence_and_removed(
    entry: &GroupConsumerEntry,
) -> (GroupPositionFence, Vec<GroupAssignmentPartition>) {
    let reconciliation = entry
        .classic_reconciliation
        .as_ref()
        .unwrap_or_else(|| panic!("prepared cooperative reconciliation"))
        .reconciliation();
    let previous = reconciliation.previous_assignment();
    (
        GroupPositionFence::new(
            previous.group_id(),
            reconciliation.previous_cycle(),
            previous.member_id(),
            previous.assignment_generation(),
        ),
        reconciliation.delta().removed().to_vec(),
    )
}

fn stage_removed_event(entry: &mut GroupConsumerEntry, removed: &[GroupAssignmentPartition]) {
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous catalog assignment"));
    let epoch = assignment.assignment_generation().get();
    let named = entry
        .catalog
        .prepare_graceful_revocation_subset_event(assignment, removed);
    entry.catalog.commit_graceful_revocation_event(named, epoch);
}
