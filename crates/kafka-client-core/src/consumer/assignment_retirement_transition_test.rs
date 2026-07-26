//! Current, stale, closed, and capacity outcomes for assignment retirement.

use crate::{Moment, NextFetchOffset, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedTopicPartition,
    AssignmentEpoch, FetchOwnership, InstallResolvedAssignment, ResolvedAssignedPartition,
    RetireAssignment, RetireAssignmentError, RetireAssignmentErrorKind,
    assignment_retirement_transition::reserve_retirement_effects,
};

#[test]
fn current_assignment_retires_in_order_without_reusing_its_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(install(None, &[(2, 3, 7), (4, 1, 11)]))
        .unwrap_or_else(|error| panic!("initial resolved assignment: {error}"));
    let active = machine.assignment_epoch();

    let retirement = machine
        .retire_assignment(RetireAssignment::new(active))
        .unwrap_or_else(|error| panic!("current assignment retirement: {error}"));

    assert_eq!(retirement.assignment_epoch(), None);
    assert!(matches!(
        retirement.effects(),
        [
            AssignedConsumerEffect::Revoke {
                assignment_epoch: first_epoch,
                partition: first,
            },
            AssignedConsumerEffect::Revoke {
                assignment_epoch: second_epoch,
                partition: second,
            },
        ] if Some(*first_epoch) == active
            && Some(*second_epoch) == active
            && *first == partition(2, 3)
            && *second == partition(4, 1)
    ));
    assert_eq!(machine.assignment_epoch(), None);

    let stale = rejected(machine.retire_assignment(RetireAssignment::new(active)));
    assert_eq!(
        stale.kind(),
        RetireAssignmentErrorKind::AssignmentEpochMismatch {
            expected: active,
            actual: None,
        }
    );

    let replacement = machine
        .install_resolved_assignment(install(None, &[(7, 0, 13)]))
        .unwrap_or_else(|error| panic!("post-retirement assignment: {error}"));
    assert_eq!(
        replacement.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
}

#[test]
fn exact_unassigned_and_empty_assignment_retirement_are_deadline_free() {
    let mut machine = AssignedConsumerMachine::new();
    let unassigned = machine
        .retire_assignment(RetireAssignment::new(None))
        .unwrap_or_else(|error| panic!("current unassigned retirement: {error}"));
    assert_eq!(unassigned.assignment_epoch(), None);
    assert!(unassigned.effects().is_empty());

    let empty = machine
        .install_resolved_assignment(install(None, &[]))
        .unwrap_or_else(|error| panic!("empty resolved assignment: {error}"));
    assert_eq!(empty.assignment_epoch(), Some(AssignmentEpoch::initial()));
    let retired = machine
        .retire_assignment(RetireAssignment::new(empty.assignment_epoch()))
        .unwrap_or_else(|error| panic!("empty assignment retirement: {error}"));
    assert_eq!(retired.assignment_epoch(), None);
    assert!(retired.effects().is_empty());
    assert_eq!(machine.assignment_epoch(), None);
}

#[test]
fn stale_optional_fences_return_the_exact_input_without_mutation() {
    let mut machine = AssignedConsumerMachine::new();
    let unexpected = RetireAssignment::new(Some(AssignmentEpoch::initial()));
    let error = rejected(machine.retire_assignment(unexpected));
    assert_eq!(
        error.kind(),
        RetireAssignmentErrorKind::AssignmentEpochMismatch {
            expected: Some(AssignmentEpoch::initial()),
            actual: None,
        }
    );
    assert_eq!(
        error.into_input().expected_assignment_epoch(),
        Some(AssignmentEpoch::initial())
    );
    assert_eq!(machine.assignment_epoch(), None);

    let installed = machine
        .install_resolved_assignment(install(None, &[(1, 0, 17)]))
        .unwrap_or_else(|error| panic!("active resolved assignment: {error}"));
    let AssignedConsumerEffect::FetchReady { fence, .. } = installed.effects()[0] else {
        panic!("active FetchReady");
    };
    let active = machine.assignment_epoch();
    let error = rejected(machine.retire_assignment(RetireAssignment::new(None)));
    assert_eq!(
        error.kind(),
        RetireAssignmentErrorKind::AssignmentEpochMismatch {
            expected: None,
            actual: active,
        }
    );
    assert_eq!(error.input().expected_assignment_epoch(), None);
    assert_eq!(machine.assignment_epoch(), active);
    assert_eq!(machine.fetch_ownership(fence), Ok(FetchOwnership::Active));
}

#[test]
fn closed_and_capacity_rejections_preserve_state_and_ownership() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(install(None, &[(1, 0, 19)]))
        .unwrap_or_else(|error| panic!("active resolved assignment: {error}"));
    let active = machine.assignment_epoch();
    machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));

    let error = rejected(machine.retire_assignment(RetireAssignment::new(active)));
    assert_eq!(error.kind(), RetireAssignmentErrorKind::ConsumerClosed);
    assert_eq!(error.input().expected_assignment_epoch(), active);
    assert_eq!(machine.assignment_epoch(), active);

    let mut effects = Vec::new();
    assert!(!reserve_retirement_effects(&mut effects, usize::MAX));
    assert!(effects.is_empty());
}

fn install(
    expected_assignment_epoch: Option<AssignmentEpoch>,
    partitions: &[(u64, u32, i64)],
) -> InstallResolvedAssignment {
    InstallResolvedAssignment::new(
        expected_assignment_epoch,
        partitions
            .iter()
            .map(|(topic, index, offset)| {
                ResolvedAssignedPartition::new(partition(*topic, *index), next_offset(*offset))
            })
            .collect(),
        Moment::from_tick(u64::MAX),
        0,
    )
}

fn rejected(
    result: Result<super::AssignedConsumerTransition, RetireAssignmentError>,
) -> RetireAssignmentError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("assignment retirement must reject"),
    }
}

fn partition(topic: u64, partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn next_offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("test offset must be nonnegative"))
}
