//! Exact timer and fencing scenarios for positive position throttles.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, PositionFence, PositionResolutionAttemptFailure,
    PositionResolutionFailure, StartPosition,
    assignment_test::{assign_at, assigned, offset},
};
use crate::{Deadline, Moment};

#[test]
fn positive_throttle_arms_exact_deadline_and_rejects_early_wake_without_mutation() {
    let mut machine = AssignedConsumerMachine::new();
    let fence = resolve_with_throttle(&mut machine, 10, 5);
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(14),
        }),
        Err(
            AssignedConsumerMachineError::PositionThrottleDeadlineNotElapsed {
                fence,
                deadline: Deadline::from_tick(15),
                now: Moment::from_tick(14),
            }
        )
    );
    let ready = machine
        .apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("exact throttle wake: {error}"));
    assert!(matches!(
        ready.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence: fetch,
            next_offset,
        }] if fetch.position() == fence && *next_offset == offset(8)
    ));
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence,
            now: Moment::from_tick(16),
        }),
        Err(AssignedConsumerMachineError::PositionThrottleNotPending { fence })
    );
}

#[test]
fn zero_throttle_becomes_fetch_ready_without_arming_a_timer() {
    let mut machine = AssignedConsumerMachine::new();
    let fence = resolving(&mut machine, Deadline::from_tick(100));
    let ready = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(8),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("unthrottled resolution: {error}"));
    assert!(matches!(
        ready.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence: fetch,
            next_offset,
        }] if fetch.position() == fence && *next_offset == offset(8)
    ));
}

#[test]
fn throttle_deadline_overflow_is_terminal_and_failed_state_is_inert_until_seek() {
    let mut machine = AssignedConsumerMachine::new();
    let fence = resolving(&mut machine, Deadline::from_tick(u64::MAX));
    let terminal = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(8),
            now: Moment::from_tick(u64::MAX - 1),
            throttle_ticks: 2,
        })
        .unwrap_or_else(|error| panic!("overflowing throttle: {error}"));
    assert_eq!(
        terminal.effects(),
        &[AssignedConsumerEffect::PositionResolutionFailed {
            fence,
            failure: PositionResolutionFailure::ThrottleDeadlineOverflow,
        }]
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolutionFailed {
            fence,
            now: Moment::from_tick(u64::MAX - 1),
            failure: PositionResolutionAttemptFailure::Transport,
        }),
        Err(AssignedConsumerMachineError::PositionResolutionNotPending { fence })
    );

    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fence.assignment_epoch(),
            partition: fence.partition(),
            position: StartPosition::Offset(offset(9)),
            now: Moment::from_tick(u64::MAX - 1),
            resolution_deadline: Deadline::from_tick(u64::MAX),
        })
        .unwrap_or_else(|error| panic!("seek recovers failed position: {error}"));
    assert!(matches!(
        seek.effects(),
        [
            AssignedConsumerEffect::Suspend { .. },
            AssignedConsumerEffect::FetchReady { next_offset, .. }
        ] if *next_offset == offset(9)
    ));
}

#[test]
fn pause_rearms_the_same_throttle_deadline_under_a_new_fence() {
    let mut machine = AssignedConsumerMachine::new();
    let old_fence = resolve_with_throttle(&mut machine, 10, 5);
    let epoch = old_fence.assignment_epoch();
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: old_fence.partition(),
        })
        .unwrap_or_else(|error| panic!("pause throttle: {error}"));
    let [
        AssignedConsumerEffect::Suspend {
            fence: paused_fence,
        },
    ] = paused.effects()
    else {
        panic!("pause must publish its fence");
    };
    let paused_fence = *paused_fence;
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence: old_fence,
            now: Moment::from_tick(15),
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: paused_fence,
            supplied: old_fence,
        })
    );

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: old_fence.partition(),
            now: Moment::from_tick(12),
            resolution_deadline: Deadline::from_tick(200),
        })
        .unwrap_or_else(|error| panic!("resume throttle: {error}"));
    assert_eq!(
        resumed.effects(),
        &[AssignedConsumerEffect::ArmPositionThrottle {
            fence: paused_fence,
            deadline: Deadline::from_tick(15),
        }]
    );
}

#[test]
fn resume_at_elapsed_throttle_and_seek_or_reassignment_fence_old_timer() {
    let mut resumed_machine = AssignedConsumerMachine::new();
    let old = resolve_with_throttle(&mut resumed_machine, 10, 5);
    let epoch = old.assignment_epoch();
    let paused = resumed_machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: old.partition(),
        })
        .unwrap_or_else(|error| panic!("pause throttle: {error}"));
    let [AssignedConsumerEffect::Suspend { fence: active }] = paused.effects() else {
        panic!("pause must publish fence");
    };
    let active = *active;
    let resumed = resumed_machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: old.partition(),
            now: Moment::from_tick(15),
            resolution_deadline: Deadline::from_tick(200),
        })
        .unwrap_or_else(|error| panic!("resume elapsed throttle: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady { fence, .. }] if fence.position() == active
    ));

    let mut replaced_machine = AssignedConsumerMachine::new();
    let stale = resolve_with_throttle(&mut replaced_machine, 10, 5);
    replaced_machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: stale.assignment_epoch(),
            partition: stale.partition(),
            position: StartPosition::Offset(offset(11)),
            now: Moment::from_tick(11),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek throttled position: {error}"));
    assert!(matches!(
        replaced_machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence: stale,
            now: Moment::from_tick(15),
        }),
        Err(AssignedConsumerMachineError::StalePosition { .. })
    ));

    let stale_assignment = resolve_with_throttle(&mut replaced_machine, 20, 5);
    assign_at(
        &mut replaced_machine,
        vec![assigned(2, 0, StartPosition::Beginning)],
        Moment::from_tick(21),
        Deadline::from_tick(100),
    );
    assert!(matches!(
        replaced_machine.apply(AssignedConsumerInput::PositionThrottleElapsed {
            fence: stale_assignment,
            now: Moment::from_tick(25),
        }),
        Err(AssignedConsumerMachineError::StaleAssignment { .. })
    ));
}

fn resolve_with_throttle(
    machine: &mut AssignedConsumerMachine,
    now: u64,
    throttle_ticks: u64,
) -> PositionFence {
    let fence = resolving(machine, Deadline::from_tick(100));
    let transition = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(8),
            now: Moment::from_tick(now),
            throttle_ticks,
        })
        .unwrap_or_else(|error| panic!("throttled resolution: {error}"));
    assert_eq!(
        transition.effects(),
        &[AssignedConsumerEffect::ArmPositionThrottle {
            fence,
            deadline: Deadline::from_tick(now + throttle_ticks),
        }]
    );
    fence
}

fn resolving(machine: &mut AssignedConsumerMachine, deadline: Deadline) -> PositionFence {
    let transition = assign_at(
        machine,
        vec![assigned(1, 0, StartPosition::Beginning)],
        Moment::from_tick(0),
        deadline,
    );
    transition
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("future beginning position must resolve"))
}
