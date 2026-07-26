//! Lossless stale, invalid, capacity, and throttle-overflow install rejection.

use crate::{Moment, NextFetchOffset, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedTopicPartition,
    AssignmentEpoch, InstallResolvedAssignment, InstallResolvedAssignmentError,
    InstallResolvedAssignmentErrorKind, ResolvedAssignedPartition,
    resolved_assignment_install::reserve_resolved_assignment_storage,
};

#[test]
fn overflow_and_invalid_order_return_the_exact_input_without_mutation() {
    let mut machine = AssignedConsumerMachine::new();
    let active = machine
        .install_resolved_assignment(install(None, &[(1, 0, 7)], 11, 0))
        .unwrap_or_else(|error| panic!("initial resolved assignment: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: active_fetch,
        ..
    } = active.effects()[0]
    else {
        panic!("initial FetchReady");
    };
    let overflow = install(machine.assignment_epoch(), &[(1, 0, 11)], u64::MAX, 1);
    let overflow_capacity = overflow.partitions_capacity();
    let error = rejected(machine.install_resolved_assignment(overflow));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::InitialFetchThrottleDeadlineOverflow
    );
    assert_eq!(error.input().partitions_capacity(), overflow_capacity);
    assert_eq!(machine.assignment_epoch(), Some(AssignmentEpoch::initial()));
    assert_eq!(
        machine.fetch_ownership(active_fetch),
        Ok(super::FetchOwnership::Active)
    );

    let mut machine = AssignedConsumerMachine::new();
    let out_of_order = install(None, &[(2, 0, 13), (1, 0, 17)], 19, 0);
    let error = rejected(machine.install_resolved_assignment(out_of_order));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::ResolvedAssignmentOutOfOrder {
            previous: topic_partition(2, 0),
            current: topic_partition(1, 0),
        }
    );
    let recovered = error.into_input();
    assert_eq!(
        recovered.partitions(),
        [resolved(2, 0, 13), resolved(1, 0, 17)]
    );
    assert_eq!(machine.assignment_epoch(), None);

    let installed = machine
        .install_resolved_assignment(install(None, &[(1, 0, 23)], 29, 0))
        .unwrap_or_else(|error| panic!("first valid assignment: {error}"));
    assert_eq!(
        installed.assignment_epoch(),
        Some(AssignmentEpoch::initial())
    );
}

#[test]
fn stale_destination_fences_return_exact_input_before_deadline_or_mutation() {
    let mut machine = AssignedConsumerMachine::new();
    let first = machine
        .install_resolved_assignment(install(None, &[(1, 0, 7)], 11, 0))
        .unwrap_or_else(|error| panic!("first resolved assignment: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch, ..
    } = first.effects()[0]
    else {
        panic!("first FetchReady");
    };
    let first_epoch = machine.assignment_epoch();

    let expected_none = install(None, &[(2, 0, 13)], u64::MAX, 1);
    let expected_none_capacity = expected_none.partitions_capacity();
    let error = rejected(machine.install_resolved_assignment(expected_none));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::ResolvedAssignmentEpochMismatch {
            expected: None,
            actual: first_epoch,
        }
    );
    let recovered = error.into_input();
    assert_eq!(recovered.expected_assignment_epoch(), None);
    assert_eq!(recovered.partitions_capacity(), expected_none_capacity);
    assert_eq!(recovered.partitions(), [resolved(2, 0, 13)]);
    assert_eq!(recovered.now(), Moment::from_tick(u64::MAX));
    assert_eq!(recovered.throttle_ticks(), 1);
    assert_eq!(
        machine.fetch_ownership(first_fetch),
        Ok(super::FetchOwnership::Active)
    );

    let replacement = machine
        .install_resolved_assignment(install(first_epoch, &[(2, 0, 17)], 19, 0))
        .unwrap_or_else(|error| panic!("matching replacement: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: replacement_fetch,
        ..
    } = replacement.effects()[1]
    else {
        panic!("replacement FetchReady after Revoke");
    };
    let second_epoch = machine.assignment_epoch();
    let wrong_some = install(first_epoch, &[(3, 0, 23)], u64::MAX, 1);
    let error = rejected(machine.install_resolved_assignment(wrong_some));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::ResolvedAssignmentEpochMismatch {
            expected: first_epoch,
            actual: second_epoch,
        }
    );
    let recovered = error.into_input();
    assert_eq!(recovered.expected_assignment_epoch(), first_epoch);
    assert_eq!(recovered.partitions(), [resolved(3, 0, 23)]);
    assert_eq!(recovered.now(), Moment::from_tick(u64::MAX));
    assert_eq!(recovered.throttle_ticks(), 1);
    assert_eq!(machine.assignment_epoch(), second_epoch);
    assert_eq!(
        machine.fetch_ownership(replacement_fetch),
        Ok(super::FetchOwnership::Active)
    );
}

#[test]
fn duplicate_closed_and_capacity_rejections_are_lossless() {
    let mut machine = AssignedConsumerMachine::new();
    let input = install(None, &[(1, 0, 11), (1, 0, 17)], 19, 0);
    let capacity = input.partitions_capacity();
    let error = rejected(machine.install_resolved_assignment(input));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::DuplicatePartition {
            partition: topic_partition(1, 0),
        }
    );
    assert_eq!(error.into_input().partitions_capacity(), capacity);
    assert_eq!(machine.assignment_epoch(), None);

    machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close unassigned machine: {error}"));
    let closed = install(None, &[(1, 0, 23)], 29, 0);
    let error = rejected(machine.install_resolved_assignment(closed));
    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::ConsumerClosed
    );
    assert_eq!(error.into_input().partitions(), [resolved(1, 0, 23)]);

    let mut states = Vec::new();
    let mut effects = Vec::new();
    assert!(!reserve_resolved_assignment_storage(
        &mut states,
        usize::MAX,
        &mut effects,
        1,
    ));
    assert!(states.is_empty());
    assert!(effects.is_empty());
    assert!(!reserve_resolved_assignment_storage(
        &mut states,
        0,
        &mut effects,
        usize::MAX,
    ));
    assert!(states.is_empty());
    assert!(effects.is_empty());
}

fn install(
    expected_assignment_epoch: Option<AssignmentEpoch>,
    partitions: &[(u64, u32, i64)],
    now: u64,
    throttle_ticks: u64,
) -> InstallResolvedAssignment {
    let mut resolved_partitions = Vec::with_capacity(partitions.len().saturating_add(3));
    resolved_partitions.extend(
        partitions
            .iter()
            .map(|(topic, partition, offset)| resolved(*topic, *partition, *offset)),
    );
    InstallResolvedAssignment::new(
        expected_assignment_epoch,
        resolved_partitions,
        Moment::from_tick(now),
        throttle_ticks,
    )
}

fn rejected(
    result: Result<super::AssignedConsumerTransition, InstallResolvedAssignmentError>,
) -> InstallResolvedAssignmentError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("resolved assignment install must reject"),
    }
}

fn resolved(topic: u64, partition: u32, raw_offset: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(topic_partition(topic, partition), offset(raw_offset))
}

fn topic_partition(topic: u64, partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("test offset must be nonnegative"))
}
