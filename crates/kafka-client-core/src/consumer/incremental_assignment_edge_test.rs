//! Empty, ordered, bounded, and atomic incremental-assignment edge scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchOwnership, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
    incremental_assignment::reserve_assignment_change,
    incremental_assignment_test::{add, fetch, maximum_assignment_epoch, remove},
};
use crate::{Deadline, Moment};

#[test]
fn empty_changes_are_open_inert_and_never_create_or_spend_an_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    let empty_add = add(&mut machine, Vec::new());
    let empty_remove = remove(&mut machine, Vec::new());
    assert_eq!(empty_add.assignment_epoch(), None);
    assert_eq!(empty_remove.assignment_epoch(), None);
    assert!(empty_add.effects().is_empty());
    assert!(empty_remove.effects().is_empty());

    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(7)))],
    );
    let epoch = initial.assignment_epoch();
    let active = fetch(initial.effects()[0]);
    assert_eq!(add(&mut machine, Vec::new()).assignment_epoch(), epoch);
    assert_eq!(remove(&mut machine, Vec::new()).assignment_epoch(), epoch);
    assert_eq!(machine.fetch_ownership(active), Ok(FetchOwnership::Active));

    machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close: {error}"));
    assert_eq!(
        machine.apply(AssignedConsumerInput::RemoveAssignments {
            partitions: Vec::new(),
        }),
        Err(AssignedConsumerMachineError::ConsumerClosed)
    );
}

#[test]
fn remove_and_add_effects_follow_caller_order_while_survivors_keep_order() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(10))),
            assigned(1, 1, StartPosition::Offset(offset(20))),
            assigned(1, 2, StartPosition::Offset(offset(30))),
        ],
    );
    let first_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let survivor = fetch(initial.effects()[1]);
    let removed = remove(&mut machine, vec![partition(1, 2), partition(1, 0)]);
    assert!(matches!(
        removed.effects(),
        [
            AssignedConsumerEffect::Revoke { assignment_epoch: third_epoch, partition: third },
            AssignedConsumerEffect::Revoke { assignment_epoch: first_epoch_effect, partition: first },
        ] if *third_epoch == first_epoch && *third == partition(1, 2)
            && *first_epoch_effect == first_epoch && *first == partition(1, 0)
    ));
    assert_eq!(
        machine.fetch_ownership(survivor),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(assigned_partitions(&machine), vec![partition(1, 1)]);

    let added = add(
        &mut machine,
        vec![
            assigned(2, 0, StartPosition::Offset(offset(40))),
            assigned(1, 0, StartPosition::Offset(offset(50))),
        ],
    );
    assert!(matches!(
        added.effects(),
        [
            AssignedConsumerEffect::FetchReady { fence: first, .. },
            AssignedConsumerEffect::FetchReady { fence: second, .. },
        ] if first.position().partition() == partition(2, 0)
            && second.position().partition() == partition(1, 0)
    ));
    assert_eq!(
        assigned_partitions(&machine),
        vec![partition(1, 1), partition(2, 0), partition(1, 0)]
    );
}

#[test]
fn remove_all_retains_an_empty_revision_and_later_add_is_fresh() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let old = fetch(initial.effects()[0]);
    let empty = remove(&mut machine, vec![partition(1, 0)]);
    let empty_epoch = empty.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    assert_eq!(machine.assignment_epoch(), Some(empty_epoch));
    assert!(assigned_partitions(&machine).is_empty());
    assert_eq!(machine.fetch_ownership(old), Ok(FetchOwnership::Superseded));

    let readded = add(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(20)))],
    );
    let new = fetch(readded.effects()[0]);
    assert!(new.position().assignment_epoch() > old.position().assignment_epoch());
    assert_eq!(machine.fetch_ownership(old), Ok(FetchOwnership::Superseded));
    assert_eq!(machine.fetch_ownership(new), Ok(FetchOwnership::Active));
}

#[test]
fn invalid_closed_exhausted_and_allocation_paths_are_atomic() {
    let mut unassigned = AssignedConsumerMachine::new();
    assert_eq!(
        unassigned.apply(AssignedConsumerInput::RemoveAssignments {
            partitions: vec![partition(1, 0)],
        }),
        Err(AssignedConsumerMachineError::NoAssignment)
    );

    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let active = fetch(initial.effects()[0]);
    let duplicate = assigned(2, 0, StartPosition::Offset(offset(20)));
    for (input, expected) in [
        (
            AssignedConsumerInput::AddAssignments {
                partitions: vec![duplicate, duplicate],
                now: Moment::from_tick(0),
                resolution_deadline: Deadline::from_tick(100),
            },
            AssignedConsumerMachineError::DuplicatePartition {
                partition: partition(2, 0),
            },
        ),
        (
            AssignedConsumerInput::AddAssignments {
                partitions: vec![assigned(1, 0, StartPosition::Offset(offset(20)))],
                now: Moment::from_tick(0),
                resolution_deadline: Deadline::from_tick(100),
            },
            AssignedConsumerMachineError::PartitionAlreadyAssigned {
                partition: partition(1, 0),
            },
        ),
        (
            AssignedConsumerInput::RemoveAssignments {
                partitions: vec![partition(1, 0), partition(1, 0)],
            },
            AssignedConsumerMachineError::DuplicatePartition {
                partition: partition(1, 0),
            },
        ),
        (
            AssignedConsumerInput::RemoveAssignments {
                partitions: vec![partition(9, 0)],
            },
            AssignedConsumerMachineError::UnknownPartition {
                partition: partition(9, 0),
            },
        ),
    ] {
        assert_eq!(machine.apply(input), Err(expected));
        assert_eq!(machine.assignment_epoch(), Some(epoch));
        assert_eq!(machine.fetch_ownership(active), Ok(FetchOwnership::Active));
    }

    machine.next_epoch = maximum_assignment_epoch();
    assert_eq!(
        machine.apply(AssignedConsumerInput::RemoveAssignments {
            partitions: vec![partition(1, 0)],
        }),
        Err(AssignedConsumerMachineError::AssignmentEpochExhausted)
    );
    assert_eq!(machine.assignment_epoch(), Some(epoch));
    assert_eq!(machine.fetch_ownership(active), Ok(FetchOwnership::Active));

    let mut values = Vec::<u8>::new();
    assert!(!reserve_assignment_change(&mut values, usize::MAX));
    assert!(values.is_empty());
}

fn assigned_partitions(machine: &AssignedConsumerMachine) -> Vec<super::AssignedTopicPartition> {
    machine
        .assignment
        .as_ref()
        .unwrap_or_else(|| panic!("assignment"))
        .partitions
        .iter()
        .map(|state| state.partition)
        .collect()
}
