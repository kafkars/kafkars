//! Reconciliation assignment coalescing and non-coalescing evidence.

use std::sync::Arc;

use crate::consumer::{
    GroupConsumerAssignment, GroupConsumerAssignmentPartition, GroupConsumerEvent,
};

use super::classic_group_event::ClassicGroupEventStore;

#[test]
fn consecutive_newer_assignments_coalesce_to_the_latest_observation() {
    let mut events = confirmed(1, 0);
    events.stage_assignment(assignment(2, 0));
    events.confirm_sync();

    assert_assigned(events.take(), 2, 0);
    assert_eq!(events.take(), None);
}

#[test]
fn stale_assignment_does_not_merge_into_a_newer_observation() {
    let mut events = confirmed(2, 0);
    events.stage_assignment(assignment(1, 0));
    events.confirm_sync();

    assert_assigned(events.take(), 2, 0);
    assert_assigned(events.take(), 1, 0);
    assert_eq!(events.take(), None);
}

#[test]
fn same_epoch_distinct_assignment_payloads_do_not_merge() {
    let mut events = confirmed(3, 0);
    events.stage_assignment(assignment(3, 1));
    events.confirm_sync();

    assert_assigned(events.take(), 3, 0);
    assert_assigned(events.take(), 3, 1);
    assert_eq!(events.take(), None);
}

fn confirmed(epoch: u64, partition: i32) -> ClassicGroupEventStore {
    let mut events = ClassicGroupEventStore::new();
    events.stage_assignment(assignment(epoch, partition));
    events.confirm_sync();
    events
}

fn assignment(epoch: u64, partition: i32) -> GroupConsumerAssignment {
    GroupConsumerAssignment::new(
        epoch,
        vec![GroupConsumerAssignmentPartition::new(
            Arc::from("orders"),
            partition,
        )],
    )
}

fn assert_assigned(event: Option<GroupConsumerEvent>, epoch: u64, partition: i32) {
    let Some(GroupConsumerEvent::PartitionsAssigned(assignment)) = event else {
        panic!("expected assigned event");
    };
    assert_eq!(assignment.assignment_epoch(), epoch);
    assert_eq!(assignment.partitions()[0].partition(), partition);
}
