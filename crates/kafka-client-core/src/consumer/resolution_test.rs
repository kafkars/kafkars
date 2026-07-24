//! Deadline, terminal, and fence scenarios for direct position resolution.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, PositionFence, PositionResolutionFailure, StartPosition,
    assignment_test::{assign_at, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn resolution_preserves_supplied_absolute_deadline_while_explicit_offset_skips_resolution() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = assign_at(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Beginning),
            assigned(1, 1, StartPosition::Offset(offset(4))),
        ],
        Moment::from_tick(10),
        Deadline::from_tick(40),
    );
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::ResolvePosition {
                deadline,
                position: StartPosition::Beginning,
                ..
            },
            AssignedConsumerEffect::FetchReady {
                next_offset,
                ..
            }
        ] if *deadline == Deadline::from_tick(40) && *next_offset == offset(4)
    ));
}

#[test]
fn already_elapsed_resolution_settles_terminally_without_interpreter_work() {
    let mut machine = AssignedConsumerMachine::new();
    let transition = assign_at(
        &mut machine,
        vec![assigned(1, 0, StartPosition::End)],
        Moment::from_tick(40),
        Deadline::from_tick(40),
    );
    let [AssignedConsumerEffect::PositionResolutionFailed { fence, failure }] =
        transition.effects()
    else {
        panic!("elapsed resolution must terminate");
    };
    assert_eq!(*failure, PositionResolutionFailure::DeadlineElapsed);
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: *fence,
            now: Moment::from_tick(41),
        }),
        Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence: *fence })
    );
}

#[test]
fn result_and_failure_observations_apply_core_owned_deadline_precedence() {
    let mut success = AssignedConsumerMachine::new();
    let success_fence = resolving(&mut success, 100);
    let transition = success
        .apply(AssignedConsumerInput::PositionResolved {
            fence: success_fence,
            next_offset: offset(8),
            now: Moment::from_tick(100),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("deadline-racing success: {error}"));
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::PositionResolutionFailed {
            fence: success_fence,
            failure: PositionResolutionFailure::DeadlineElapsed,
        }]
    );

    let mut failed_before = AssignedConsumerMachine::new();
    let before_fence = resolving(&mut failed_before, 100);
    let before = failed_before
        .apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: before_fence,
            now: Moment::from_tick(99),
        })
        .unwrap_or_else(|error| panic!("failure before deadline: {error}"));
    assert_eq!(
        before.effects(),
        &[AssignedConsumerEffect::PositionResolutionFailed {
            fence: before_fence,
            failure: PositionResolutionFailure::AttemptFailed,
        }]
    );

    let mut failed_at = AssignedConsumerMachine::new();
    let at_fence = resolving(&mut failed_at, 100);
    let at = failed_at
        .apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: at_fence,
            now: Moment::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("failure at deadline: {error}"));
    assert!(matches!(
        at.effects(),
        [AssignedConsumerEffect::PositionResolutionFailed {
            failure: PositionResolutionFailure::DeadlineElapsed,
            ..
        }]
    ));
}

#[test]
fn early_deadline_wake_rejects_without_consuming_the_live_resolution() {
    let mut machine = AssignedConsumerMachine::new();
    let fence = resolving(&mut machine, 100);
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence,
            now: Moment::from_tick(99),
        }),
        Err(
            AssignedConsumerMachineError::PositionResolutionDeadlineNotElapsed {
                fence,
                deadline: Deadline::from_tick(100),
                now: Moment::from_tick(99),
            }
        )
    );
    let terminal = machine
        .apply(AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence,
            now: Moment::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("exact deadline wake: {error}"));
    assert!(matches!(
        terminal.effects(),
        [AssignedConsumerEffect::PositionResolutionFailed {
            failure: PositionResolutionFailure::DeadlineElapsed,
            ..
        }]
    ));
}

#[test]
fn wrong_phase_terminals_are_inert_and_seek_remains_the_only_local_recovery() {
    let mut machine = AssignedConsumerMachine::new();
    let fence = resolving(&mut machine, 100);
    machine
        .apply(AssignedConsumerInput::PositionResolutionFailed {
            fence,
            now: Moment::from_tick(10),
        })
        .unwrap_or_else(|error| panic!("terminal resolution failure: {error}"));

    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(3),
            now: Moment::from_tick(11),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence })
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence,
            now: Moment::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence })
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::PositionThrottleNotPending { fence })
    );

    let recovered = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fence.assignment_epoch(),
            partition: fence.partition(),
            position: StartPosition::Offset(offset(4)),
            now: Moment::from_tick(12),
            resolution_deadline: Deadline::from_tick(120),
        })
        .unwrap_or_else(|error| panic!("seek failed position: {error}"));
    assert!(matches!(
        recovered.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. }
        ] if *next_offset == offset(4)
    ));
}

#[test]
fn pause_and_reassignment_fence_every_old_resolution_terminal() {
    let mut machine = AssignedConsumerMachine::new();
    let old_fence = resolving(&mut machine, 100);
    let epoch = old_fence.assignment_epoch();
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("pause resolving partition: {error}"));
    let [AssignedConsumerEffect::Suspend { fence: active }] = paused.effects() else {
        panic!("pause must publish the new position fence");
    };
    for input in [
        AssignedConsumerInput::PositionResolved {
            fence: old_fence,
            next_offset: offset(1),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        },
        AssignedConsumerInput::PositionResolutionFailed {
            fence: old_fence,
            now: Moment::from_tick(1),
        },
        AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence: old_fence,
            now: Moment::from_tick(100),
        },
    ] {
        assert_eq!(
            machine.apply(input),
            Err(AssignedConsumerMachineError::StalePosition {
                active: *active,
                supplied: old_fence,
            })
        );
    }

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: partition(1, 0),
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(120),
        })
        .unwrap_or_else(|error| panic!("resume resolution: {error}"));
    let [
        AssignedConsumerEffect::ResolvePosition {
            fence: resumed_fence,
            deadline,
            ..
        },
    ] = resumed.effects()
    else {
        panic!("resume must start a newly fenced resolution");
    };
    assert_eq!(*deadline, Deadline::from_tick(120));

    assign_at(
        &mut machine,
        vec![assigned(1, 0, StartPosition::End)],
        Moment::from_tick(3),
        Deadline::from_tick(130),
    );
    assert!(matches!(
        machine.apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: *resumed_fence,
            now: Moment::from_tick(4),
        }),
        Err(AssignedConsumerMachineError::StaleAssignment { .. })
    ));
}

fn resolving(machine: &mut AssignedConsumerMachine, deadline: u64) -> PositionFence {
    let transition = assign_at(
        machine,
        vec![assigned(1, 0, StartPosition::Beginning)],
        Moment::from_tick(0),
        Deadline::from_tick(deadline),
    );
    let [AssignedConsumerEffect::ResolvePosition { fence, .. }] = transition.effects() else {
        panic!("future beginning position must resolve");
    };
    *fence
}
