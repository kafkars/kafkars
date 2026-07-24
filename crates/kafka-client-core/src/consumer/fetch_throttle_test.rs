//! Successful-Fetch throttle timing and re-fencing scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchFence, FetchRevision, StartPosition,
    assignment_test::{assign, assigned, offset},
};
use crate::{Deadline, Moment};

#[test]
fn positive_fetch_throttle_arms_one_exact_future_fence_and_rejects_early_wake() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let armed = advance_with_throttle(&mut machine, completed, 12, 10, 5);
    assert_eq!(armed.revision().get(), 2);
    let wrong_revision = FetchFence::new(
        armed.position(),
        FetchRevision::try_from_raw_for_test(3)
            .unwrap_or_else(|| panic!("test revision is nonzero")),
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: wrong_revision,
            now: Moment::from_tick(15),
        }),
        Err(AssignedConsumerMachineError::StaleFetch {
            supplied: wrong_revision,
        })
    );

    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: armed,
            now: Moment::from_tick(14),
        }),
        Err(
            AssignedConsumerMachineError::FetchThrottleDeadlineNotElapsed {
                fence: armed,
                deadline: Deadline::from_tick(15),
                now: Moment::from_tick(14),
            }
        )
    );
    let ready = machine
        .apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: armed,
            now: Moment::from_tick(15),
        })
        .unwrap_or_else(|error| panic!("exact Fetch throttle wake: {error}"));
    assert_eq!(
        ready.effects(),
        &[AssignedConsumerEffect::FetchReady {
            fence: armed,
            next_offset: offset(12),
        }]
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: armed,
            now: Moment::from_tick(16),
        }),
        Err(AssignedConsumerMachineError::FetchThrottleNotPending { fence: armed })
    );
}

#[test]
fn zero_fetch_throttle_emits_the_next_exact_revision_without_a_timer() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            next_offset: offset(12),
            now: Moment::from_tick(10),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("unthrottled Fetch progress: {error}"));
    assert!(matches!(
        advanced.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.position() == completed.position()
            && fence.revision().get() == 2
            && *next_offset == offset(12)
    ));
}

#[test]
fn pause_rearms_the_same_fetch_throttle_deadline_under_a_new_fence() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let old_timer = advance_with_throttle(&mut machine, completed, 12, 10, 5);
    let epoch = old_timer.position().assignment_epoch();
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: old_timer.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause Fetch throttle: {error}"));
    let [
        AssignedConsumerEffect::Suspend {
            fence: paused_fence,
        },
    ] = paused.effects()
    else {
        panic!("pause must publish the replacement position fence");
    };
    let paused_fence = *paused_fence;
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchThrottleElapsed {
            fence: old_timer,
            now: Moment::from_tick(15),
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: paused_fence,
            supplied: old_timer.position(),
        })
    );

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: old_timer.position().partition(),
            now: Moment::from_tick(12),
            resolution_deadline: Deadline::from_tick(200),
        })
        .unwrap_or_else(|error| panic!("resume Fetch throttle: {error}"));
    let [
        AssignedConsumerEffect::ArmFetchThrottle {
            fence: active_timer,
            deadline,
        },
    ] = resumed.effects()
    else {
        panic!("resume before the deadline must rearm Fetch throttle");
    };
    assert_eq!(active_timer.position(), paused_fence);
    assert_eq!(active_timer.revision().get(), 1);
    assert_eq!(*deadline, Deadline::from_tick(15));
}

#[test]
fn resume_after_fetch_throttle_elapsed_issues_new_fenced_fetch_immediately() {
    let mut machine = AssignedConsumerMachine::new();
    let completed = first_fetch(&mut machine);
    let old_timer = advance_with_throttle(&mut machine, completed, 12, 10, 5);
    let epoch = old_timer.position().assignment_epoch();
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: old_timer.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause Fetch throttle: {error}"));
    let [
        AssignedConsumerEffect::Suspend {
            fence: paused_fence,
        },
    ] = paused.effects()
    else {
        panic!("pause must publish the replacement position fence");
    };

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: old_timer.position().partition(),
            now: Moment::from_tick(15),
            resolution_deadline: Deadline::from_tick(200),
        })
        .unwrap_or_else(|error| panic!("resume elapsed Fetch throttle: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.position() == *paused_fence
            && fence.revision().get() == 1
            && *next_offset == offset(12)
    ));
}

pub(super) fn first_fetch(machine: &mut AssignedConsumerMachine) -> FetchFence {
    let assigned = assign(
        machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    assigned
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("explicit offset must begin one Fetch"))
}

pub(super) fn advance_with_throttle(
    machine: &mut AssignedConsumerMachine,
    completed: FetchFence,
    next_offset: i64,
    now: u64,
    throttle_ticks: u64,
) -> FetchFence {
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: completed,
            next_offset: offset(next_offset),
            now: Moment::from_tick(now),
            throttle_ticks,
        })
        .unwrap_or_else(|error| panic!("successful throttled Fetch: {error}"));
    let [AssignedConsumerEffect::ArmFetchThrottle { fence, deadline }] = transition.effects()
    else {
        panic!("positive Fetch throttle must arm one timer");
    };
    assert_eq!(*deadline, Deadline::from_tick(now + throttle_ticks));
    *fence
}
