//! Atomic caller-ordered batch pause and retained-position resume evidence.

use super::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    DeliveryOwnership, FetchRevision, PositionEpoch, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn empty_batch_is_an_inert_success_without_an_assignment() {
    let mut machine = AssignedConsumerMachine::new();

    let paused = machine
        .pause_partitions(super::AssignmentEpoch::initial(), &[])
        .unwrap_or_else(|error| panic!("empty pause: {error}"));
    let resumed = machine
        .resume_retained_partitions(
            super::AssignmentEpoch::initial(),
            &[],
            Moment::from_tick(7),
            Deadline::from_tick(70),
        )
        .unwrap_or_else(|error| panic!("empty resume: {error}"));

    assert_eq!(paused.assignment_epoch(), None);
    assert!(paused.effects().is_empty());
    assert_eq!(resumed.assignment_epoch(), None);
    assert!(resumed.effects().is_empty());
    assert_eq!(machine.assignment_epoch(), None);
}

#[test]
fn duplicate_and_unknown_pause_reject_before_any_partition_mutates() {
    let (mut machine, epoch, old) = assigned_pair();
    let first = partition(1, 0);
    let unknown = partition(9, 9);

    assert_eq!(
        machine.pause_partitions(epoch, &[first, first]),
        Err(AssignedConsumerMachineError::DuplicatePartition { partition: first })
    );
    assert_eq!(
        machine.pause_partitions(epoch, &[first, unknown]),
        Err(AssignedConsumerMachineError::UnknownPartition { partition: unknown })
    );
    for fence in old {
        assert_eq!(
            machine.delivery_ownership(fence),
            Ok(DeliveryOwnership::Active)
        );
    }
}

#[test]
fn pause_preserves_caller_order_fences_old_fetch_and_is_idempotent() {
    let (mut machine, epoch, old) = assigned_pair();
    let targets = [partition(1, 1), partition(1, 0)];

    let paused = machine
        .pause_partitions(epoch, &targets)
        .unwrap_or_else(|error| panic!("batch pause: {error}"));
    assert_eq!(paused.effects().len(), 2);
    for (effect, target) in paused.effects().iter().zip(targets) {
        assert!(matches!(
            effect,
            AssignedConsumerEffect::Suspend { fence } if fence.partition() == target
        ));
    }
    for fence in old {
        assert_eq!(
            machine.delivery_ownership(fence),
            Ok(DeliveryOwnership::Superseded)
        );
    }

    let repeated = machine
        .pause_partitions(epoch, &targets)
        .unwrap_or_else(|error| panic!("idempotent pause: {error}"));
    assert!(repeated.effects().is_empty());
}

#[test]
fn pause_epoch_exhaustion_is_preflighted_across_the_complete_batch() {
    let (mut machine, epoch, old) = assigned_pair();
    let assignment = machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("active assignment"));
    assignment.partitions[1].replace_position_epoch_for_test(
        PositionEpoch::try_from_raw_for_test(u64::MAX)
            .unwrap_or_else(|| panic!("maximum nonzero position epoch")),
    );

    assert_eq!(
        machine.pause_partitions(epoch, &[partition(1, 0), partition(1, 1)]),
        Err(AssignedConsumerMachineError::PositionEpochExhausted {
            partition: partition(1, 1),
        })
    );
    assert_eq!(
        machine.delivery_ownership(old[0]),
        Ok(DeliveryOwnership::Active)
    );
}

#[test]
fn resume_preflights_every_fetch_revision_and_restores_retained_offsets_in_order() {
    let (mut machine, epoch, old) = assigned_pair();
    let targets = [partition(1, 1), partition(1, 0)];
    machine
        .pause_partitions(epoch, &targets)
        .unwrap_or_else(|error| panic!("pause before resume: {error}"));
    let assignment = machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("active assignment"));
    assignment.partitions[0].replace_fetch_revision_for_test(
        FetchRevision::try_from_raw_for_test(u64::MAX)
            .unwrap_or_else(|| panic!("maximum nonzero Fetch revision")),
    );

    assert_eq!(
        machine.resume_retained_partitions(
            epoch,
            &targets,
            Moment::from_tick(9),
            Deadline::from_tick(90),
        ),
        Err(AssignedConsumerMachineError::FetchRevisionExhausted {
            partition: partition(1, 0),
        })
    );
    for fence in old {
        assert_eq!(
            machine.delivery_ownership(fence),
            Ok(DeliveryOwnership::Superseded)
        );
    }

    let assignment = machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("active assignment"));
    assignment.partitions[0].replace_fetch_revision_for_test(FetchRevision::after_initial());
    let resumed = machine
        .resume_retained_partitions(
            epoch,
            &targets,
            Moment::from_tick(10),
            Deadline::from_tick(100),
        )
        .unwrap_or_else(|error| panic!("resume retained batch: {error}"));
    assert!(matches!(
        resumed.effects(),
        [
            AssignedConsumerEffect::FetchReady {
                next_offset: second,
                ..
            },
            AssignedConsumerEffect::FetchReady {
                next_offset: first,
                ..
            },
        ] if *second == offset(20) && *first == offset(10)
    ));
}

fn assigned_pair() -> (
    AssignedConsumerMachine,
    super::AssignmentEpoch,
    [super::FetchFence; 2],
) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(10))),
            assigned(1, 1, StartPosition::Offset(offset(20))),
        ],
    );
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch"));
    let mut fences = transition
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        });
    let first = fences.next().unwrap_or_else(|| panic!("first Fetch"));
    let second = fences.next().unwrap_or_else(|| panic!("second Fetch"));
    (machine, epoch, [first, second])
}
