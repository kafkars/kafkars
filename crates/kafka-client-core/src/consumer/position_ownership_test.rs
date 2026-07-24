//! Directional ownership scenarios for queued position resolution.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, PositionFence, PositionOwnership, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn active_resolution_is_superseded_by_control_terminal_and_assignment_progress() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(&mut machine, vec![assigned(1, 3, StartPosition::Beginning)]);
    let initial = resolve_fence(initial.effects()[0]);
    assert_eq!(
        machine.position_ownership(initial),
        Ok(PositionOwnership::Active)
    );

    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: initial.assignment_epoch(),
            partition: initial.partition(),
        })
        .unwrap_or_else(|error| panic!("pause resolution: {error}"));
    let AssignedConsumerEffect::Suspend {
        fence: paused_fence,
    } = paused.effects()[0]
    else {
        panic!("pause must expose the replacement fence");
    };
    assert_eq!(
        machine.position_ownership(initial),
        Ok(PositionOwnership::Superseded)
    );
    assert_eq!(
        machine.position_ownership(paused_fence),
        Err(AssignedConsumerMachineError::PositionResolutionNotPending {
            fence: paused_fence,
        })
    );

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: initial.assignment_epoch(),
            partition: initial.partition(),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume resolution: {error}"));
    let resumed = resolve_fence(resumed.effects()[0]);
    assert_eq!(
        machine.position_ownership(resumed),
        Ok(PositionOwnership::Active)
    );
    machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence: resumed,
            next_offset: offset(10),
            now: Moment::from_tick(2),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("complete resolution: {error}"));
    assert_eq!(
        machine.position_ownership(resumed),
        Ok(PositionOwnership::Superseded)
    );

    assign(&mut machine, vec![assigned(1, 4, StartPosition::End)]);
    assert_eq!(
        machine.position_ownership(resumed),
        Ok(PositionOwnership::Superseded)
    );
}

#[test]
fn future_and_cross_partition_fences_remain_directional_errors() {
    let mut active = AssignedConsumerMachine::new();
    let active_transition = assign(&mut active, vec![assigned(1, 3, StartPosition::Beginning)]);
    let active_fence = resolve_fence(active_transition.effects()[0]);

    let mut advanced = AssignedConsumerMachine::new();
    let first = assign(
        &mut advanced,
        vec![assigned(1, 3, StartPosition::Beginning)],
    );
    let first = resolve_fence(first.effects()[0]);
    let seek = advanced
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: first.assignment_epoch(),
            partition: first.partition(),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("advance comparison position: {error}"));
    let future_position = resolve_fence(seek.effects()[1]);
    assert_eq!(
        active.position_ownership(future_position),
        Err(AssignedConsumerMachineError::StalePosition {
            active: active_fence,
            supplied: future_position,
        })
    );

    let replacement = assign(&mut advanced, vec![assigned(1, 3, StartPosition::End)]);
    let future_assignment = resolve_fence(replacement.effects()[1]);
    assert_eq!(
        active.position_ownership(future_assignment),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active: active_fence.assignment_epoch(),
            supplied: future_assignment.assignment_epoch(),
        })
    );

    let mut other_partition = AssignedConsumerMachine::new();
    let other = assign(
        &mut other_partition,
        vec![assigned(1, 4, StartPosition::Beginning)],
    );
    let other = resolve_fence(other.effects()[0]);
    assert_eq!(
        active.position_ownership(other),
        Err(AssignedConsumerMachineError::UnknownPartition {
            partition: partition(1, 4),
        })
    );
}

const fn resolve_fence(effect: AssignedConsumerEffect) -> PositionFence {
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = effect else {
        panic!("position-resolution effect");
    };
    fence
}
