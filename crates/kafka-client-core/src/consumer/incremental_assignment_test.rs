//! Stable survivor-fence and remove/re-add scenarios for incremental assignment.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, AssignedConsumerTransition, AssignedPartition,
    AssignedTopicPartition, AssignmentEpoch, DeliveryOwnership, FetchFailure, FetchFence,
    FetchOwnership, FetchRecords, PositionOwnership, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn unrelated_add_preserves_resolution_fetch_delivery_and_offset_fences() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(10))),
            assigned(1, 1, StartPosition::Beginning),
        ],
    );
    let acquisition_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let delivery = fetch(initial.effects()[0]);
    let AssignedConsumerEffect::ResolvePosition {
        fence: resolution, ..
    } = initial.effects()[1]
    else {
        panic!("symbolic resolution");
    };
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: delivery,
            records: FetchRecords::Deliverable,
            next_offset: offset(12),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance retained partition: {error}"));
    let active_fetch = fetch(
        *advanced
            .effects()
            .last()
            .unwrap_or_else(|| panic!("next fetch")),
    );

    let added = add(
        &mut machine,
        vec![assigned(2, 0, StartPosition::Offset(offset(30)))],
    );
    let current_epoch = added.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    assert!(current_epoch > acquisition_epoch);
    assert!(matches!(
        added.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.position().assignment_epoch() == current_epoch
                && *next_offset == offset(30)
    ));
    assert_eq!(
        machine.fetch_ownership(active_fetch),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(
        machine.position_ownership(resolution),
        Ok(PositionOwnership::Active)
    );
    assert_eq!(
        machine.delivery_ownership(delivery),
        Ok(DeliveryOwnership::Active)
    );

    let survivor = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: active_fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(13),
            now: Moment::from_tick(2),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("settle unchanged fetch: {error}"));
    assert_eq!(survivor.assignment_epoch(), Some(current_epoch));
    assert!(matches!(
        survivor.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.position().assignment_epoch() == acquisition_epoch
                && *next_offset == offset(13)
    ));
}

#[test]
fn remove_and_readd_never_revive_old_work_or_perturb_survivors() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(10))),
            assigned(1, 1, StartPosition::Offset(offset(20))),
        ],
    );
    let old_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let old_fetch = fetch(initial.effects()[0]);
    let survivor = fetch(initial.effects()[1]);

    let removed = remove(&mut machine, vec![partition(1, 0)]);
    let removal_epoch = removed
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    assert!(matches!(
        removed.effects(),
        [AssignedConsumerEffect::Revoke { assignment_epoch, partition: removed }]
            if *assignment_epoch == old_epoch && *removed == partition(1, 0)
    ));
    assert_eq!(
        machine.fetch_ownership(old_fetch),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(
        machine.fetch_ownership(survivor),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchFailed {
            fence: old_fetch,
            failure: FetchFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active: removal_epoch,
            supplied: old_epoch,
        })
    );

    let readded = add(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(99)))],
    );
    let readd_epoch = readded
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let new_fetch = fetch(readded.effects()[0]);
    assert!(readd_epoch > removal_epoch);
    assert_eq!(new_fetch.position().assignment_epoch(), readd_epoch);
    assert_eq!(
        machine.fetch_ownership(old_fetch),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(
        machine.fetch_ownership(new_fetch),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(
        machine.fetch_ownership(survivor),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchFailed {
            fence: old_fetch,
            failure: FetchFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active: readd_epoch,
            supplied: old_epoch,
        })
    );
}

#[test]
fn controls_use_current_revision_but_emit_the_survivor_acquisition_epoch() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let acquisition_epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("epoch"));
    let added = add(
        &mut machine,
        vec![assigned(2, 0, StartPosition::Offset(offset(20)))],
    );
    let current_epoch = added.assignment_epoch().unwrap_or_else(|| panic!("epoch"));
    assert_eq!(
        machine.apply(AssignedConsumerInput::Pause {
            assignment_epoch: acquisition_epoch,
            partition: partition(1, 0),
        }),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active: current_epoch,
            supplied: acquisition_epoch,
        })
    );
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: current_epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("pause survivor: {error}"));
    assert_eq!(paused.assignment_epoch(), Some(current_epoch));
    assert!(matches!(
        paused.effects(),
        [AssignedConsumerEffect::Suspend { fence }]
            if fence.assignment_epoch() == acquisition_epoch
    ));
}

#[test]
fn symbolic_add_preserves_the_exact_call_boundary_resolution_deadline() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let survivor = fetch(initial.effects()[0]);
    let added = machine
        .apply(AssignedConsumerInput::AddAssignments {
            partitions: vec![assigned(2, 0, StartPosition::Beginning)],
            now: Moment::from_tick(17),
            resolution_deadline: Deadline::from_tick(29),
        })
        .unwrap_or_else(|error| panic!("symbolic incremental add: {error}"));
    let added_epoch = added.assignment_epoch().unwrap_or_else(|| panic!("epoch"));

    assert!(matches!(
        added.effects(),
        [AssignedConsumerEffect::ResolvePosition { fence, position, deadline }]
            if fence.assignment_epoch() == added_epoch
                && fence.partition() == partition(2, 0)
                && *position == StartPosition::Beginning
                && *deadline == Deadline::from_tick(29)
    ));
    assert_eq!(
        machine.fetch_ownership(survivor),
        Ok(FetchOwnership::Active)
    );
}

pub(super) fn add(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedPartition>,
) -> AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::AddAssignments {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("incremental add: {error}"))
}

pub(super) fn remove(
    machine: &mut AssignedConsumerMachine,
    partitions: Vec<AssignedTopicPartition>,
) -> AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::RemoveAssignments { partitions })
        .unwrap_or_else(|error| panic!("incremental remove: {error}"))
}

pub(super) const fn fetch(effect: AssignedConsumerEffect) -> FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

pub(super) fn maximum_assignment_epoch() -> AssignmentEpoch {
    AssignmentEpoch::try_from_raw_for_test(u64::MAX).unwrap_or_else(|| panic!("maximum epoch"))
}
