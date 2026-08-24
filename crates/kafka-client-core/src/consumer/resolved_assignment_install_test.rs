//! Atomic resolved-assignment installation and initial Fetch-throttle scenarios.

use crate::{Deadline, Moment, NextFetchOffset, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, FetchFence, FetchRevision, InstallResolvedAssignment,
    PositionEpoch, PositionFence, ResolvedAssignedPartition, StartPosition,
};

#[test]
fn zero_throttle_installs_one_epoch_and_emits_ordered_fetch_ready() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .install_resolved_assignment(install(None, &[(1, 0, 11), (1, 2, 17)], 23, 0))
        .unwrap_or_else(|error| panic!("resolved assignment should install: {error}"));

    assert_eq!(
        transition.assignment_epoch(),
        Some(AssignmentEpoch::initial())
    );
    assert_eq!(
        transition.effects(),
        [
            fetch_ready(1, 0, 11, AssignmentEpoch::initial()),
            fetch_ready(1, 2, 17, AssignmentEpoch::initial()),
        ]
    );
    assert_eq!(machine.assignment_epoch(), Some(AssignmentEpoch::initial()));
}

#[test]
fn positive_throttle_arms_exact_initial_fetch_fences_until_due() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .install_resolved_assignment(install(None, &[(1, 0, 11), (2, 1, 17)], 23, 7))
        .unwrap_or_else(|error| panic!("throttled assignment should install: {error}"));
    let effects = transition.effects();
    assert_eq!(effects.len(), 2);
    for (effect, expected_partition) in effects
        .iter()
        .zip([topic_partition(1, 0), topic_partition(2, 1)])
    {
        let AssignedConsumerEffect::ArmFetchThrottle { fence, deadline } = effect else {
            panic!("positive initial throttle must arm Fetch");
        };
        assert_eq!(fence.position().partition(), expected_partition);
        assert_eq!(
            fence.position().assignment_epoch(),
            AssignmentEpoch::initial()
        );
        assert_eq!(fence.revision().get(), 1);
        assert_eq!(*deadline, Deadline::from_tick(30));
    }

    let AssignedConsumerEffect::ArmFetchThrottle { fence, .. } = effects[0] else {
        panic!("first throttle effect");
    };
    let ready = machine
        .apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence,
            now: Moment::from_tick(30),
        })
        .unwrap_or_else(|error| panic!("exact initial throttle should elapse: {error}"));
    assert_eq!(
        ready.effects(),
        [fetch_ready(1, 0, 11, AssignmentEpoch::initial())]
    );
}

#[test]
fn replacement_revokes_each_acquisition_epoch_before_resolved_fetches() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                topic_partition(1, 0),
                StartPosition::Offset(offset(5)),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("initial direct assignment: {error}"));
    let addition = machine
        .apply(AssignedConsumerInput::AddAssignments {
            partitions: vec![AssignedPartition::new(
                topic_partition(1, 2),
                StartPosition::Offset(offset(9)),
            )],
            now: Moment::from_tick(11),
            resolution_deadline: Deadline::from_tick(13),
        })
        .unwrap_or_else(|error| panic!("incremental addition: {error}"));
    let addition_epoch = addition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("addition epoch"));

    let transition = machine
        .install_resolved_assignment(install(machine.assignment_epoch(), &[(2, 1, 13)], 17, 0))
        .unwrap_or_else(|error| panic!("resolved replacement: {error}"));

    assert_eq!(
        transition.assignment_epoch().map(AssignmentEpoch::get),
        Some(3)
    );
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Revoke {
                assignment_epoch: first_epoch,
                partition: first_partition,
            },
            AssignedConsumerEffect::Revoke {
                assignment_epoch: second_epoch,
                partition: second_partition,
            },
            AssignedConsumerEffect::FetchReady { fence, next_offset },
        ] if first_epoch.get() == 1
            && *first_partition == topic_partition(1, 0)
            && *second_epoch == addition_epoch
            && *second_partition == topic_partition(1, 2)
            && fence.position().assignment_epoch().get() == 3
            && fence.position().partition() == topic_partition(2, 1)
            && *next_offset == offset(13)
    ));
}

#[test]
fn empty_install_allocates_a_fresh_epoch_without_fetch_or_deadline_work() {
    let mut machine = AssignedConsumerMachine::new();
    let first = machine
        .install_resolved_assignment(install(None, &[], u64::MAX, 1))
        .unwrap_or_else(|error| panic!("empty resolved assignment: {error}"));
    assert_eq!(first.assignment_epoch(), Some(AssignmentEpoch::initial()));
    assert!(first.effects().is_empty());

    let replacement = machine
        .install_resolved_assignment(install(machine.assignment_epoch(), &[], u64::MAX, u64::MAX))
        .unwrap_or_else(|error| panic!("empty replacement: {error}"));
    assert_eq!(
        replacement.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
    assert!(replacement.effects().is_empty());
    assert_eq!(
        machine.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
}

#[test]
fn nonempty_to_empty_replacement_revokes_old_order_without_fetch_work() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(install(None, &[(1, 0, 7), (1, 2, 11)], 13, 0))
        .unwrap_or_else(|error| panic!("nonempty resolved assignment: {error}"));
    let previous_epoch = machine.assignment_epoch();

    let transition = machine
        .install_resolved_assignment(install(previous_epoch, &[], u64::MAX, u64::MAX))
        .unwrap_or_else(|error| panic!("bound-empty replacement: {error}"));

    assert_eq!(
        transition.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::Revoke {
                assignment_epoch: first_epoch,
                partition: first,
            },
            AssignedConsumerEffect::Revoke {
                assignment_epoch: second_epoch,
                partition: second,
            },
        ] if Some(*first_epoch) == previous_epoch
            && Some(*second_epoch) == previous_epoch
            && *first == topic_partition(1, 0)
            && *second == topic_partition(1, 2)
    ));
    assert_eq!(
        machine.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
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

fn resolved(topic: u64, partition: u32, raw_offset: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(topic_partition(topic, partition), offset(raw_offset))
}

fn fetch_ready(
    topic: u64,
    partition: u32,
    raw_offset: i64,
    assignment_epoch: AssignmentEpoch,
) -> AssignedConsumerEffect {
    let position = PositionFence::new(
        assignment_epoch,
        topic_partition(topic, partition),
        PositionEpoch::initial(),
    );
    AssignedConsumerEffect::FetchReady {
        fence: FetchFence::new(position, FetchRevision::initial()),
        next_offset: offset(raw_offset),
    }
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
