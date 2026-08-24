//! Heterogeneous cleanup and deliberate full-reconciliation fencing scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, FetchRecords,
    ReconcileResolvedAssignment, ResolvedAssignmentTarget, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
    incremental_assignment_test::{add, fetch, remove},
};
use crate::{Deadline, Moment};

#[test]
fn close_and_full_replacement_revoke_each_partition_acquisition_epoch() {
    let mut closing = AssignedConsumerMachine::new();
    let initial = assign(
        &mut closing,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let first_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let added = add(
        &mut closing,
        vec![assigned(2, 0, StartPosition::Offset(offset(20)))],
    );
    let second_epoch = added.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    let closed = closing
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close heterogeneous assignment: {error}"));
    assert_eq!(closed.assignment_epoch(), Some(second_epoch));
    assert!(matches!(
        closed.effects(),
        [
            AssignedConsumerEffect::AcceptClose { .. },
            AssignedConsumerEffect::Suspend { fence: first_suspend },
            AssignedConsumerEffect::Revoke { assignment_epoch: first_revoke, partition: first },
            AssignedConsumerEffect::Suspend { fence: second_suspend },
            AssignedConsumerEffect::Revoke { assignment_epoch: second_revoke, partition: second },
        ] if first_suspend.assignment_epoch() == first_epoch
            && *first_revoke == first_epoch && *first == partition(1, 0)
            && second_suspend.assignment_epoch() == second_epoch
            && *second_revoke == second_epoch && *second == partition(2, 0)
    ));

    let mut replacing = AssignedConsumerMachine::new();
    let initial = assign(
        &mut replacing,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let first_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let second = add(
        &mut replacing,
        vec![assigned(2, 0, StartPosition::Offset(offset(20)))],
    );
    let second_epoch = second.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    let replacement = replacing
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(3, 0, StartPosition::Offset(offset(30)))],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("full replacement: {error}"));
    let replacement_epoch = replacement
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    assert!(matches!(
        replacement.effects(),
        [
            AssignedConsumerEffect::Revoke { assignment_epoch: first_revoke, partition: first },
            AssignedConsumerEffect::Revoke { assignment_epoch: second_revoke, partition: second },
            AssignedConsumerEffect::FetchReady { fence, next_offset },
        ] if *first_revoke == first_epoch && *first == partition(1, 0)
            && *second_revoke == second_epoch && *second == partition(2, 0)
            && fence.position().assignment_epoch() == replacement_epoch
            && *next_offset == offset(30)
    ));
}

#[test]
fn resolved_reconciliation_deliberately_refreshes_every_retained_state_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(10))),
            assigned(1, 1, StartPosition::Offset(offset(20))),
        ],
    );
    let first_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let added = add(
        &mut machine,
        vec![assigned(2, 0, StartPosition::Offset(offset(30)))],
    );
    let second_epoch = added.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    let reconciled = machine
        .reconcile_resolved_assignment(ReconcileResolvedAssignment::new(
            second_epoch,
            vec![
                ResolvedAssignmentTarget::Retain(partition(1, 0)),
                ResolvedAssignmentTarget::Retain(partition(1, 1)),
                ResolvedAssignmentTarget::Retain(partition(2, 0)),
            ],
            Moment::from_tick(1),
            0,
        ))
        .unwrap_or_else(|error| panic!("full reconciliation: {error}"));
    let refreshed = reconciled
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    assert!(refreshed > second_epoch);
    assert!(matches!(
        reconciled.effects(),
        [
            AssignedConsumerEffect::Suspend { fence: first },
            AssignedConsumerEffect::Suspend { fence: second },
            AssignedConsumerEffect::Suspend { fence: third },
            AssignedConsumerEffect::FetchReady { fence: first_fetch, .. },
            AssignedConsumerEffect::FetchReady { fence: second_fetch, .. },
            AssignedConsumerEffect::FetchReady { fence: third_fetch, .. },
        ] if first.assignment_epoch() == first_epoch
            && second.assignment_epoch() == first_epoch
            && third.assignment_epoch() == second_epoch
            && first_fetch.position().assignment_epoch() == refreshed
            && second_fetch.position().assignment_epoch() == refreshed
            && third_fetch.position().assignment_epoch() == refreshed
    ));
    let assignment = machine
        .assignment
        .as_ref()
        .unwrap_or_else(|| panic!("assignment"));
    assert!(
        assignment
            .partitions
            .iter()
            .all(|state| state.assignment_epoch() == refreshed)
    );
}

#[test]
fn pause_and_fetch_throttle_survive_unrelated_changes_exactly() {
    let mut paused = AssignedConsumerMachine::new();
    let initial = assign(
        &mut paused,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let first_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let transition = paused
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: first_epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    let paused_fence = match transition.effects() {
        [AssignedConsumerEffect::Suspend { fence }] => *fence,
        _ => panic!("pause fence"),
    };
    let added = add(
        &mut paused,
        vec![assigned(2, 0, StartPosition::Offset(offset(20)))],
    );
    let current = remove(&mut paused, vec![partition(2, 0)])
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    assert!(current > added.assignment_epoch().unwrap_or_else(|| panic!("epoch")));
    let resumed = paused
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: current,
            partition: partition(1, 0),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume survivor: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.position() == paused_fence && *next_offset == offset(10)
    ));

    let mut throttled = AssignedConsumerMachine::new();
    let initial = assign(
        &mut throttled,
        vec![assigned(3, 0, StartPosition::Offset(offset(10)))],
    );
    let active = fetch(initial.effects()[0]);
    let transition = throttled
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: active,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("throttle fetch: {error}"));
    let throttled_fence = match transition.effects() {
        [AssignedConsumerEffect::ArmFetchThrottle { fence, deadline }]
            if *deadline == Deadline::from_tick(15) =>
        {
            *fence
        }
        _ => panic!("fetch throttle"),
    };
    let current = add(
        &mut throttled,
        vec![assigned(4, 0, StartPosition::Offset(offset(40)))],
    )
    .assignment_epoch();
    let elapsed = throttled
        .apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: throttled_fence,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("elapsed survivor throttle: {error}"));
    assert_eq!(elapsed.assignment_epoch(), current);
    assert!(matches!(
        elapsed.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if *fence == throttled_fence && *next_offset == offset(12)
    ));
}
