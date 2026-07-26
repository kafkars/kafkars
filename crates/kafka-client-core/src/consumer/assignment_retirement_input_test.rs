//! Reusable classic-group retirement input, fencing, and stale-terminal evidence.

use crate::{Moment, NextFetchOffset, PartitionIndex, TopicId};

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedTopicPartition, AssignmentEpoch, FetchFailure,
    FetchOwnership, InstallResolvedAssignment, ResolvedAssignedPartition,
    RetireAssignmentErrorKind,
};

#[test]
fn reusable_retirement_revokes_in_order_and_preserves_the_next_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(install(None, &[(2, 3, 7), (4, 1, 11)]))
        .unwrap_or_else(|error| panic!("initial resolved assignment: {error}"));
    let active = machine.assignment_epoch();

    let retirement = machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: active,
        })
        .unwrap_or_else(|error| panic!("reusable assignment retirement: {error}"));

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

    let replacement = machine
        .install_resolved_assignment(install(None, &[(7, 0, 13)]))
        .unwrap_or_else(|error| panic!("post-retirement assignment: {error}"));
    assert_eq!(
        replacement.assignment_epoch().map(AssignmentEpoch::get),
        Some(2)
    );
}

#[test]
fn reusable_retirement_fences_unassigned_stale_and_future_epochs() {
    let mut machine = AssignedConsumerMachine::new();
    let unassigned = machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: None,
        })
        .unwrap_or_else(|error| panic!("exact unassigned retirement: {error}"));
    assert!(unassigned.effects().is_empty());

    assert_eq!(
        machine.apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: Some(AssignmentEpoch::initial()),
        }),
        Err(AssignedConsumerMachineError::AssignmentRetirementRejected {
            kind: RetireAssignmentErrorKind::AssignmentEpochMismatch {
                expected: Some(AssignmentEpoch::initial()),
                actual: None,
            },
        })
    );

    let first = machine
        .install_resolved_assignment(install(None, &[(1, 0, 17)]))
        .unwrap_or_else(|error| panic!("first resolved assignment: {error}"));
    let first_epoch = first
        .assignment_epoch()
        .unwrap_or_else(|| panic!("first assignment epoch"));
    let second = machine
        .install_resolved_assignment(install(Some(first_epoch), &[(1, 0, 19)]))
        .unwrap_or_else(|error| panic!("second resolved assignment: {error}"));
    let active = second
        .assignment_epoch()
        .unwrap_or_else(|| panic!("second assignment epoch"));
    let future = active
        .checked_next()
        .unwrap_or_else(|| panic!("future assignment epoch"));

    for supplied in [first_epoch, future] {
        assert_eq!(
            machine.apply(AssignedConsumerInput::RetireAssignment {
                assignment_epoch: Some(supplied),
            }),
            Err(AssignedConsumerMachineError::AssignmentRetirementRejected {
                kind: RetireAssignmentErrorKind::AssignmentEpochMismatch {
                    expected: Some(supplied),
                    actual: Some(active),
                },
            })
        );
        assert_eq!(machine.assignment_epoch(), Some(active));
    }
}

#[test]
fn retired_fetch_terminal_is_inert_before_and_after_rejoin() {
    let mut machine = AssignedConsumerMachine::new();
    let installed = machine
        .install_resolved_assignment(install(None, &[(3, 0, 23)]))
        .unwrap_or_else(|error| panic!("initial resolved assignment: {error}"));
    let AssignedConsumerEffect::FetchReady { fence: retired, .. } = installed.effects()[0] else {
        panic!("initial FetchReady");
    };
    let retired_epoch = installed.assignment_epoch();

    machine
        .apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: retired_epoch,
        })
        .unwrap_or_else(|error| panic!("assignment retirement: {error}"));
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchFailed {
            fence: retired,
            failure: FetchFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::NoAssignment)
    );

    let rejoined = machine
        .install_resolved_assignment(install(None, &[(3, 0, 29)]))
        .unwrap_or_else(|error| panic!("rejoined resolved assignment: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: replacement, ..
    } = rejoined.effects()[0]
    else {
        panic!("rejoined FetchReady");
    };
    let active = replacement.position().assignment_epoch();

    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchFailed {
            fence: retired,
            failure: FetchFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active,
            supplied: retired.position().assignment_epoch(),
        })
    );
    assert_eq!(
        machine.fetch_ownership(retired),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(
        machine.fetch_ownership(replacement),
        Ok(FetchOwnership::Active)
    );
}

#[test]
fn reusable_retirement_rejects_closed_without_mutation() {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .install_resolved_assignment(install(None, &[(5, 0, 31)]))
        .unwrap_or_else(|error| panic!("active resolved assignment: {error}"));
    let active = machine.assignment_epoch();
    machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));

    assert_eq!(
        machine.apply(AssignedConsumerInput::RetireAssignment {
            assignment_epoch: active,
        }),
        Err(AssignedConsumerMachineError::ConsumerClosed)
    );
    assert_eq!(machine.assignment_epoch(), active);
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
