//! Assignment- and position-fenced pause, resume, and seek scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchRecords, PositionResolutionAttemptFailure, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn pause_fences_inflight_fetch_and_resume_restarts_retained_offset() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let AssignedConsumerEffect::FetchReady {
        fence: old_fetch,
        next_offset: old_offset,
    } = initial.effects()[0]
    else {
        panic!("explicit offset should fetch immediately");
    };
    let epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assigned transition epoch"));

    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: partition(1, 0),
        })
        .unwrap_or_else(|error| panic!("pause partition: {error}"));
    let AssignedConsumerEffect::Suspend {
        fence: paused_fence,
    } = paused.effects()[0]
    else {
        panic!("pause should publish its new fence");
    };
    assert!(paused_fence.position_epoch() > old_fetch.position().position_epoch());
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence: old_fetch,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(12),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: paused_fence,
            supplied: old_fetch.position(),
        })
    );

    let resumed = machine
        .apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: partition(1, 0),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("resume partition: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.position() == paused_fence && *next_offset == old_offset
    ));
}

#[test]
fn seek_orders_fence_before_replacement_and_rejects_old_work() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(&mut machine, vec![assigned(3, 2, StartPosition::Beginning)]);
    let AssignedConsumerEffect::ResolvePosition {
        fence: old_fence, ..
    } = initial.effects()[0]
    else {
        panic!("beginning should require resolution");
    };
    let epoch = initial
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assigned transition epoch"));

    let seek = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: epoch,
            partition: partition(3, 2),
            position: StartPosition::End,
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek to end: {error}"));
    let [
        AssignedConsumerEffect::Suspend { fence: new_fence },
        AssignedConsumerEffect::ResolvePosition {
            fence: resolution_fence,
            position: StartPosition::End,
            ..
        },
    ] = seek.effects()
    else {
        panic!("seek should fence before resolving replacement");
    };
    assert_eq!(new_fence, resolution_fence);
    assert!(new_fence.position_epoch() > old_fence.position_epoch());
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolved {
            fence: old_fence,
            next_offset: offset(4),
            now: Moment::from_tick(2),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: *new_fence,
            supplied: old_fence,
        })
    );
    for input in [
        AssignedConsumerInput::PositionResolutionFailed {
            fence: old_fence,
            now: Moment::from_tick(2),
            failure: PositionResolutionAttemptFailure::Transport,
        },
        AssignedConsumerInput::PositionResolutionDeadlineElapsed {
            fence: old_fence,
            now: Moment::from_tick(100),
        },
    ] {
        assert_eq!(
            machine.apply(input),
            Err(AssignedConsumerMachineError::StalePosition {
                active: *new_fence,
                supplied: old_fence,
            })
        );
    }
}

#[test]
fn controls_from_a_superseded_assignment_cannot_mutate_replacement() {
    let mut machine = AssignedConsumerMachine::new();
    let old_epoch = assign(&mut machine, vec![assigned(1, 0, StartPosition::Beginning)])
        .assignment_epoch()
        .unwrap_or_else(|| panic!("old assignment epoch"));
    let active_epoch = assign(&mut machine, vec![assigned(1, 0, StartPosition::End)])
        .assignment_epoch()
        .unwrap_or_else(|| panic!("active assignment epoch"));

    assert_eq!(
        machine.apply(AssignedConsumerInput::Pause {
            assignment_epoch: old_epoch,
            partition: partition(1, 0),
        }),
        Err(AssignedConsumerMachineError::StaleAssignment {
            active: active_epoch,
            supplied: old_epoch,
        })
    );
}
