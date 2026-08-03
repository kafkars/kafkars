//! Lossless reconciliation input and atomic rejection scenarios.

use super::{
    InstallResolvedAssignment, ReconcileResolvedAssignment, ReconcileResolvedAssignmentError,
    ReconcileResolvedAssignmentErrorKind, ResolvedAssignedPartition, ResolvedAssignmentTarget,
    reconciliation_transition::{
        PreparedReconciliationTarget, reserve_reconciliation_storage,
        reserve_reconciliation_targets,
    },
};
use crate::consumer::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedTopicPartition,
    AssignmentEpoch, FetchOwnership, NextFetchOffset, PositionEpoch,
};
use crate::{Moment, PartitionIndex, TopicId};

#[test]
fn linear_input_accessors_and_error_recover_exact_target_capacity() {
    let epoch = AssignmentEpoch::initial();
    let mut targets = Vec::with_capacity(7);
    targets.push(retain(1, 0));
    targets.push(acquire(2, 0, 11));
    let target_capacity = targets.capacity();
    let input = ReconcileResolvedAssignment::new(epoch, targets, Moment::from_tick(13), 17);
    assert_eq!(input.expected_assignment_epoch(), epoch);
    assert_eq!(input.targets(), [retain(1, 0), acquire(2, 0, 11)]);
    assert_eq!(input.targets_capacity(), target_capacity);
    assert_eq!(input.now(), Moment::from_tick(13));
    assert_eq!(input.acquired_throttle_ticks(), 17);

    let error = ReconcileResolvedAssignmentError::new(
        ReconcileResolvedAssignmentErrorKind::ConsumerClosed,
        input,
    );
    assert_eq!(
        error.kind(),
        ReconcileResolvedAssignmentErrorKind::ConsumerClosed
    );
    assert_eq!(error.input().targets_capacity(), target_capacity);
    let (recovered_epoch, recovered, now, throttle) = error.into_input().into_parts();
    assert_eq!(recovered_epoch, epoch);
    assert_eq!(recovered, [retain(1, 0), acquire(2, 0, 11)]);
    assert_eq!(recovered.capacity(), target_capacity);
    assert_eq!(now, Moment::from_tick(13));
    assert_eq!(throttle, 17);
}

#[test]
fn stale_missing_existing_duplicate_and_order_rejections_are_atomic() {
    let (mut machine, epoch, active_fetch) = active();
    let stale = AssignmentEpoch::try_from_raw_for_test(epoch.get() + 1)
        .unwrap_or_else(|| panic!("next test epoch"));
    reject_kind(
        &mut machine,
        reconcile(stale, vec![retain(1, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::AssignmentEpochMismatch {
            expected: stale,
            actual: Some(epoch),
        },
    );
    reject_kind(
        &mut machine,
        reconcile(epoch, vec![retain(2, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::RetainedPartitionMissing {
            partition: partition(2, 0),
        },
    );
    reject_kind(
        &mut machine,
        reconcile(epoch, vec![acquire(1, 0, 13)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::AcquiredPartitionAlreadyExists {
            partition: partition(1, 0),
        },
    );
    reject_kind(
        &mut machine,
        reconcile(epoch, vec![retain(1, 0), acquire(1, 0, 17)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::DuplicatePartition {
            partition: partition(1, 0),
        },
    );
    reject_kind(
        &mut machine,
        reconcile(epoch, vec![acquire(2, 0, 17), retain(1, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::TargetOutOfOrder {
            previous: partition(2, 0),
            current: partition(1, 0),
        },
    );
    assert_eq!(machine.assignment_epoch(), Some(epoch));
    assert_eq!(
        machine.fetch_ownership(active_fetch),
        Ok(FetchOwnership::Active)
    );
}

#[test]
fn closed_absent_position_and_assignment_exhaustion_preserve_input_and_state() {
    let mut absent = AssignedConsumerMachine::new();
    let expected = AssignmentEpoch::initial();
    reject_kind(
        &mut absent,
        reconcile(expected, Vec::new(), u64::MAX, u64::MAX),
        ReconcileResolvedAssignmentErrorKind::AssignmentEpochMismatch {
            expected,
            actual: None,
        },
    );

    let (mut closed, epoch, _) = active();
    closed
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));
    reject_kind(
        &mut closed,
        reconcile(epoch, vec![retain(1, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::ConsumerClosed,
    );

    let (mut position_exhausted, epoch, _) = active();
    position_exhausted
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("assignment"))
        .partitions[0]
        .replace_position_epoch_for_test(
            PositionEpoch::try_from_raw_for_test(u64::MAX)
                .unwrap_or_else(|| panic!("maximum position epoch")),
        );
    reject_kind(
        &mut position_exhausted,
        reconcile(epoch, vec![retain(1, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::PositionEpochExhausted {
            partition: partition(1, 0),
        },
    );
    assert_eq!(position_exhausted.assignment_epoch(), Some(epoch));
    let retained_position = position_exhausted
        .assignment
        .as_ref()
        .unwrap_or_else(|| panic!("assignment"))
        .partitions[0]
        .position_fence(epoch);
    assert_eq!(retained_position.position_epoch().get(), u64::MAX);

    let (mut assignment_exhausted, epoch, active_fetch) = active();
    assignment_exhausted.next_epoch = AssignmentEpoch::try_from_raw_for_test(u64::MAX)
        .unwrap_or_else(|| panic!("maximum assignment epoch"));
    reject_kind(
        &mut assignment_exhausted,
        reconcile(epoch, vec![retain(1, 0)], 0, 0),
        ReconcileResolvedAssignmentErrorKind::AssignmentEpochExhausted,
    );
    assert_eq!(assignment_exhausted.assignment_epoch(), Some(epoch));
    assert_eq!(
        assignment_exhausted.fetch_ownership(active_fetch),
        Ok(FetchOwnership::Active)
    );
}

#[test]
fn acquired_throttle_overflow_precedes_allocation_and_is_ignored_without_acquire() {
    let (mut machine, epoch, active_fetch) = active();
    let mut targets = Vec::with_capacity(5);
    targets.push(retain(1, 0));
    targets.push(acquire(2, 0, 13));
    let input = ReconcileResolvedAssignment::new(epoch, targets, Moment::from_tick(u64::MAX), 1);
    let capacity = input.targets_capacity();
    let error = rejected(machine.reconcile_resolved_assignment(input));
    assert_eq!(
        error.kind(),
        ReconcileResolvedAssignmentErrorKind::AcquiredFetchThrottleDeadlineOverflow
    );
    assert_eq!(error.into_input().targets_capacity(), capacity);
    assert_eq!(machine.assignment_epoch(), Some(epoch));
    assert_eq!(
        machine.fetch_ownership(active_fetch),
        Ok(FetchOwnership::Active)
    );

    machine
        .reconcile_resolved_assignment(reconcile(epoch, vec![retain(1, 0)], u64::MAX, 1))
        .unwrap_or_else(|error| panic!("throttle is irrelevant without acquisition: {error}"));
}

#[test]
fn all_reconciliation_storage_reservations_are_fallible_before_mutation() {
    let mut targets: Vec<PreparedReconciliationTarget> = Vec::new();
    assert!(!reserve_reconciliation_targets(&mut targets, usize::MAX));
    assert!(targets.is_empty());

    let mut states = Vec::new();
    let mut effects = Vec::new();
    assert!(!reserve_reconciliation_storage(
        &mut states,
        usize::MAX,
        &mut effects,
        1,
    ));
    assert!(states.is_empty());
    assert!(effects.is_empty());
    assert!(!reserve_reconciliation_storage(
        &mut states,
        0,
        &mut effects,
        usize::MAX,
    ));
    assert!(states.is_empty());
    assert!(effects.is_empty());
}

fn active() -> (
    AssignedConsumerMachine,
    AssignmentEpoch,
    crate::consumer::FetchFence,
) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .install_resolved_assignment(InstallResolvedAssignment::new(
            None,
            vec![resolved(1, 0, 7)],
            Moment::from_tick(0),
            0,
        ))
        .unwrap_or_else(|error| panic!("active assignment: {error}"));
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let fetch = match transition.effects() {
        [AssignedConsumerEffect::FetchReady { fence, .. }] => *fence,
        _ => panic!("one FetchReady"),
    };
    (machine, epoch, fetch)
}

fn reject_kind(
    machine: &mut AssignedConsumerMachine,
    input: ReconcileResolvedAssignment,
    expected: ReconcileResolvedAssignmentErrorKind,
) {
    let targets = input.targets().to_vec();
    let error = rejected(machine.reconcile_resolved_assignment(input));
    assert_eq!(error.kind(), expected);
    assert_eq!(error.into_input().targets(), targets);
}

fn rejected(
    result: Result<crate::consumer::AssignedConsumerTransition, ReconcileResolvedAssignmentError>,
) -> ReconcileResolvedAssignmentError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("reconciliation must reject"),
    }
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

fn resolved(topic: u64, raw_partition: u32, value: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(partition(topic, raw_partition), offset(value))
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
