//! Retained-position movement, effect ordering, and fresh-fence scenarios.

#![expect(
    clippy::manual_let_else,
    reason = "fixture matches name each expected effect while retaining assertion context"
)]

use super::{
    InstallResolvedAssignment, ReconcileResolvedAssignment, ResolvedAssignedPartition,
    ResolvedAssignmentTarget,
};
use crate::consumer::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, FetchFailure, FetchOwnership, FetchRecords,
    NextFetchOffset, PositionOwnership, PositionResolutionAttemptFailure,
    PositionResolutionFailure, StartPosition,
};
use crate::{Deadline, Moment, PartitionIndex, TopicId};

#[test]
fn retain_remove_and_acquire_preserve_offset_and_effect_order() {
    let mut machine = AssignedConsumerMachine::new();
    let installed = machine
        .install_resolved_assignment(install(None, &[(1, 0, 10), (1, 1, 20), (1, 2, 30)]))
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let old_epoch = installed
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let retained_fetch = match installed.effects()[1] {
        AssignedConsumerEffect::FetchReady { fence, .. } => fence,
        _ => panic!("middle FetchReady"),
    };
    machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: retained_fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(25),
            now: Moment::from_tick(2),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance retained offset: {error}"));

    let transition = machine
        .reconcile_resolved_assignment(reconcile(
            old_epoch,
            vec![retain(1, 1), acquire(2, 0, 40)],
            3,
            0,
        ))
        .unwrap_or_else(|error| panic!("reconcile assignment: {error}"));
    let new_epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("new epoch"));
    assert_eq!(new_epoch.get(), old_epoch.get() + 1);
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Revoke { assignment_epoch: first_epoch, partition: first },
            AssignedConsumerEffect::Revoke { assignment_epoch: second_epoch, partition: second },
            AssignedConsumerEffect::Suspend { fence: suspend },
            AssignedConsumerEffect::FetchReady { fence: retained, next_offset: retained_offset },
            AssignedConsumerEffect::FetchReady { fence: acquired, next_offset: acquired_offset },
        ] if *first_epoch == old_epoch && *first == partition(1, 0)
            && *second_epoch == old_epoch && *second == partition(1, 2)
            && suspend.assignment_epoch() == old_epoch && suspend.partition() == partition(1, 1)
            && retained.position().assignment_epoch() == new_epoch
            && retained.position().partition() == partition(1, 1)
            && *retained_offset == offset(25)
            && acquired.position().assignment_epoch() == new_epoch
            && acquired.position().partition() == partition(2, 0)
            && *acquired_offset == offset(40)
    ));
}

#[test]
fn paused_partition_stays_inert_and_resumes_under_new_epoch() {
    let (mut machine, old_epoch, old_fetch) = one_fetch(1, 0, 11);
    machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: old_epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("pause retained partition: {error}"));
    let transition = machine
        .reconcile_resolved_assignment(reconcile(old_epoch, vec![retain(1, 0)], 4, 0))
        .unwrap_or_else(|error| panic!("reconcile paused partition: {error}"));
    let new_epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("new epoch"));
    assert!(matches!(
        transition.effects(),
        [AssignedConsumerEffect::Suspend { fence }]
            if fence.assignment_epoch() == old_epoch && fence.partition() == partition(1, 0)
    ));
    assert_eq!(
        machine.fetch_ownership(old_fetch),
        Ok(FetchOwnership::Superseded)
    );

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: new_epoch,
            partition: partition(1, 0),
            now: Moment::from_tick(5),
            resolution_deadline: Deadline::from_tick(99),
        })
        .unwrap_or_else(|error| panic!("resume retained partition: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.position().assignment_epoch() == new_epoch && *next_offset == offset(11)
    ));
}

#[test]
fn active_symbolic_resolution_reuses_original_deadline_and_becomes_due() {
    let (mut machine, old_epoch, old_fence) = symbolic(Deadline::from_tick(100));
    let transition = machine
        .reconcile_resolved_assignment(reconcile(old_epoch, vec![retain(1, 0)], 10, 0))
        .unwrap_or_else(|error| panic!("retain active resolution: {error}"));
    let new_epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("new epoch"));
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Suspend { fence: suspend },
            AssignedConsumerEffect::ResolvePosition { fence, position: StartPosition::Beginning, deadline },
        ] if suspend.assignment_epoch() == old_epoch
            && fence.assignment_epoch() == new_epoch
            && *deadline == Deadline::from_tick(100)
    ));
    assert_eq!(
        machine.position_ownership(old_fence),
        Ok(PositionOwnership::Superseded)
    );

    let transition = machine
        .reconcile_resolved_assignment(reconcile(new_epoch, vec![retain(1, 0)], 100, 0))
        .unwrap_or_else(|error| panic!("expire retained resolution: {error}"));
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::PositionResolutionFailed {
                failure: PositionResolutionFailure::DeadlineElapsed,
                ..
            },
        ]
    ));
}

#[test]
fn retained_position_and_fetch_throttles_preserve_deadlines_and_due_policy() {
    let (mut position_pending, epoch, position_fence) = symbolic(Deadline::from_tick(100));
    position_pending
        .apply(AssignedConsumerInput::PositionResolved {
            fence: position_fence,
            next_offset: offset(8),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("position throttle: {error}"));
    let pending = position_pending
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(1, 0)], 14, 0))
        .unwrap_or_else(|error| panic!("pending position throttle: {error}"));
    assert!(matches!(
        pending.effects(),
        [AssignedConsumerEffect::Suspend { .. }, AssignedConsumerEffect::ArmPositionThrottle { deadline, .. }]
            if *deadline == Deadline::from_tick(15)
    ));

    let (mut position_due, epoch, position_fence) = symbolic(Deadline::from_tick(100));
    position_due
        .apply(AssignedConsumerInput::PositionResolved {
            fence: position_fence,
            next_offset: offset(8),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("position throttle: {error}"));
    let due = position_due
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(1, 0)], 15, 0))
        .unwrap_or_else(|error| panic!("due position throttle: {error}"));
    assert!(matches!(
        due.effects(),
        [AssignedConsumerEffect::Suspend { .. }, AssignedConsumerEffect::FetchReady { next_offset, .. }]
            if *next_offset == offset(8)
    ));

    let (mut fetch_pending, epoch, fetch) = one_fetch(2, 0, 10);
    fetch_pending
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(20),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("fetch throttle: {error}"));
    let pending = fetch_pending
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(2, 0)], 24, 0))
        .unwrap_or_else(|error| panic!("pending fetch throttle: {error}"));
    assert!(matches!(
        pending.effects(),
        [AssignedConsumerEffect::Suspend { .. }, AssignedConsumerEffect::ArmFetchThrottle { deadline, .. }]
            if *deadline == Deadline::from_tick(25)
    ));

    let (mut fetch_due, epoch, fetch) = one_fetch(2, 0, 10);
    fetch_due
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(20),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("fetch throttle: {error}"));
    let due = fetch_due
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(2, 0)], 25, 0))
        .unwrap_or_else(|error| panic!("due fetch throttle: {error}"));
    assert!(matches!(
        due.effects(),
        [AssignedConsumerEffect::Suspend { .. }, AssignedConsumerEffect::FetchReady { next_offset, .. }]
            if *next_offset == offset(12)
    ));
}

#[test]
fn acquired_partition_uses_the_one_preflighted_throttle_deadline() {
    let (mut machine, epoch, _) = one_fetch(1, 0, 10);
    let transition = machine
        .reconcile_resolved_assignment(reconcile(
            epoch,
            vec![retain(1, 0), acquire(2, 0, 20)],
            30,
            7,
        ))
        .unwrap_or_else(|error| panic!("throttled acquisition: {error}"));
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset: retained, .. },
            AssignedConsumerEffect::ArmFetchThrottle { fence, deadline },
        ] if *retained == offset(10)
            && fence.position().partition() == partition(2, 0)
            && *deadline == Deadline::from_tick(37)
    ));
}

#[test]
fn failed_positions_remain_inert_across_fresh_assignment_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![
                AssignedPartition::new(partition(1, 0), StartPosition::Beginning),
                AssignedPartition::new(partition(1, 1), StartPosition::Offset(offset(7))),
            ],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("mixed assignment: {error}"));
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let resolution = match transition.effects()[0] {
        AssignedConsumerEffect::ResolvePosition { fence, .. } => fence,
        _ => panic!("resolution"),
    };
    let fetch = match transition.effects()[1] {
        AssignedConsumerEffect::FetchReady { fence, .. } => fence,
        _ => panic!("fetch"),
    };
    machine
        .apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: resolution,
            now: Moment::from_tick(1),
            failure: PositionResolutionAttemptFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("fail resolution: {error}"));
    machine
        .apply(AssignedConsumerInput::FetchFailed {
            fence: fetch,
            failure: FetchFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("fail fetch: {error}"));

    let transition = machine
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(1, 0), retain(1, 1)], 2, 0))
        .unwrap_or_else(|error| panic!("retain failed positions: {error}"));
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::Suspend { .. }
        ]
    ));
}

#[test]
fn empty_target_then_reacquisition_never_reuses_an_epoch() {
    let (mut machine, first_epoch, first_fetch) = one_fetch(1, 0, 9);
    let empty = machine
        .reconcile_resolved_assignment(reconcile(first_epoch, Vec::new(), u64::MAX, u64::MAX))
        .unwrap_or_else(|error| panic!("empty reconciliation: {error}"));
    let empty_epoch = empty
        .assignment_epoch()
        .unwrap_or_else(|| panic!("empty epoch"));
    assert_eq!(empty_epoch.get(), first_epoch.get() + 1);
    assert!(matches!(
        empty.effects(),
        [AssignedConsumerEffect::Revoke { assignment_epoch, partition: revoked }]
            if *assignment_epoch == first_epoch && *revoked == partition(1, 0)
    ));

    let acquired = machine
        .reconcile_resolved_assignment(reconcile(empty_epoch, vec![acquire(1, 0, 19)], 3, 0))
        .unwrap_or_else(|error| panic!("reacquire removed partition: {error}"));
    let acquired_epoch = acquired
        .assignment_epoch()
        .unwrap_or_else(|| panic!("acquired epoch"));
    assert_eq!(acquired_epoch.get(), empty_epoch.get() + 1);
    let new_fetch = match acquired.effects() {
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if *next_offset == offset(19) =>
        {
            *fence
        }
        _ => panic!("one reacquired FetchReady"),
    };
    assert_eq!(
        machine.fetch_ownership(first_fetch),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(
        machine.fetch_ownership(new_fetch),
        Ok(FetchOwnership::Active)
    );
}

fn symbolic(
    deadline: Deadline,
) -> (
    AssignedConsumerMachine,
    AssignmentEpoch,
    crate::consumer::PositionFence,
) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                partition(1, 0),
                StartPosition::Beginning,
            )],
            now: Moment::from_tick(0),
            resolution_deadline: deadline,
        })
        .unwrap_or_else(|error| panic!("symbolic assignment: {error}"));
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let fence = match transition.effects() {
        [AssignedConsumerEffect::ResolvePosition { fence, .. }] => *fence,
        _ => panic!("one resolution"),
    };
    (machine, epoch, fence)
}

fn one_fetch(
    topic: u64,
    raw_partition: u32,
    raw_offset: i64,
) -> (
    AssignedConsumerMachine,
    AssignmentEpoch,
    crate::consumer::FetchFence,
) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .install_resolved_assignment(install(None, &[(topic, raw_partition, raw_offset)]))
        .unwrap_or_else(|error| panic!("one fetch assignment: {error}"));
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let fence = match transition.effects() {
        [AssignedConsumerEffect::FetchReady { fence, .. }] => *fence,
        _ => panic!("one fetch"),
    };
    (machine, epoch, fence)
}

fn install(
    expected: Option<AssignmentEpoch>,
    partitions: &[(u64, u32, i64)],
) -> InstallResolvedAssignment {
    InstallResolvedAssignment::new(
        expected,
        partitions
            .iter()
            .map(|&(topic, partition, offset)| resolved(topic, partition, offset))
            .collect(),
        Moment::from_tick(0),
        0,
    )
}

fn reconcile(
    expected: AssignmentEpoch,
    targets: Vec<ResolvedAssignmentTarget>,
    now: u64,
    throttle: u64,
) -> ReconcileResolvedAssignment {
    ReconcileResolvedAssignment::new(expected, targets, Moment::from_tick(now), throttle)
}

const fn retain(topic: u64, partition: u32) -> ResolvedAssignmentTarget {
    ResolvedAssignmentTarget::Retain(AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    ))
}

fn acquire(topic: u64, partition: u32, value: i64) -> ResolvedAssignmentTarget {
    ResolvedAssignmentTarget::Acquire(resolved(topic, partition, value))
}

fn resolved(topic: u64, partition: u32, value: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(
        AssignedTopicPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        offset(value),
    )
}

const fn partition(topic: u64, partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
