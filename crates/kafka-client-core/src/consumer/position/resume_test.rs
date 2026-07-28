//! Atomic batch resumption of symbolic positions and retained position throttles.

use super::super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchRevision, PositionResolutionAttemptFailure, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn symbolic_positions_install_exact_resolutions_in_caller_order() {
    let (mut machine, epoch) = assigned_pair();
    let beginning = partition(1, 0);
    let end = partition(1, 1);
    machine
        .pause_partitions(epoch, &[beginning, end])
        .unwrap_or_else(|error| panic!("pause before symbolic seeks: {error}"));
    let beginning_fence =
        seek_while_paused(&mut machine, epoch, beginning, StartPosition::Beginning);
    let end_fence = seek_while_paused(&mut machine, epoch, end, StartPosition::End);

    let resolution_deadline = Deadline::from_tick(120);
    let resumed = machine
        .resume_retained_partitions(
            epoch,
            &[end, beginning],
            Moment::from_tick(20),
            resolution_deadline,
        )
        .unwrap_or_else(|error| panic!("resume symbolic positions: {error}"));

    assert!(matches!(
        resumed.effects(),
        [
            AssignedConsumerEffect::ResolvePosition {
                fence: first_fence,
                position: StartPosition::End,
                deadline: first_deadline,
            },
            AssignedConsumerEffect::ResolvePosition {
                fence: second_fence,
                position: StartPosition::Beginning,
                deadline: second_deadline,
            },
        ] if *first_fence == end_fence
            && *second_fence == beginning_fence
            && *first_deadline == resolution_deadline
            && *second_deadline == resolution_deadline
    ));
}

#[test]
fn later_rejection_leaves_an_earlier_symbolic_resolution_uninstalled() {
    let (mut machine, epoch) = assigned_pair();
    let symbolic = partition(1, 0);
    let retained_offset = partition(1, 1);
    let targets = [symbolic, retained_offset];
    machine
        .pause_partitions(epoch, &targets)
        .unwrap_or_else(|error| panic!("pause before mixed resume: {error}"));
    let symbolic_fence = seek_while_paused(&mut machine, epoch, symbolic, StartPosition::Beginning);
    let assignment = machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("active assignment"));
    assignment.partitions[1].replace_fetch_revision_for_test(
        FetchRevision::try_from_raw_for_test(u64::MAX)
            .unwrap_or_else(|| panic!("maximum nonzero Fetch revision")),
    );

    assert_eq!(
        machine.resume_retained_partitions(
            epoch,
            &targets,
            Moment::from_tick(30),
            Deadline::from_tick(130),
        ),
        Err(AssignedConsumerMachineError::FetchRevisionExhausted {
            partition: retained_offset,
        })
    );

    let assignment = machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("active assignment"));
    assignment.partitions[1].replace_fetch_revision_for_test(FetchRevision::after_initial());
    let retry_deadline = Deadline::from_tick(150);
    let resumed = machine
        .resume_retained_partitions(epoch, &targets, Moment::from_tick(40), retry_deadline)
        .unwrap_or_else(|error| panic!("retry mixed resume: {error}"));
    assert!(matches!(
        resumed.effects(),
        [
            AssignedConsumerEffect::ResolvePosition {
                fence,
                position: StartPosition::Beginning,
                deadline,
            },
            AssignedConsumerEffect::FetchReady {
                next_offset,
                ..
            },
        ] if *fence == symbolic_fence
            && *deadline == retry_deadline
            && *next_offset == offset(20)
    ));
}

#[test]
fn boundary_now_rearms_or_releases_a_retained_position_throttle() {
    let (mut pending, epoch, target, pending_fence) = paused_position_throttle();
    let rearmed = pending
        .resume_retained_partitions(
            epoch,
            &[target],
            Moment::from_tick(14),
            Deadline::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("resume pending position throttle: {error}"));
    assert_eq!(
        rearmed.effects(),
        &[AssignedConsumerEffect::ArmPositionThrottle {
            fence: pending_fence,
            deadline: Deadline::from_tick(15),
        }]
    );

    let (mut elapsed, epoch, target, elapsed_fence) = paused_position_throttle();
    let released = elapsed
        .resume_retained_partitions(
            epoch,
            &[target],
            Moment::from_tick(15),
            Deadline::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("resume elapsed position throttle: {error}"));
    assert!(matches!(
        released.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.position() == elapsed_fence && *next_offset == offset(8)
    ));
}

#[test]
fn failed_symbolic_position_rejects_resume_until_seek_replaces_it() {
    let mut machine = AssignedConsumerMachine::new();
    let target = partition(3, 0);
    let transition = assign(&mut machine, vec![assigned(3, 0, StartPosition::Beginning)]);
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch"));
    let [AssignedConsumerEffect::ResolvePosition { fence, .. }] = transition.effects() else {
        panic!("beginning position must start one resolution");
    };
    machine
        .apply(AssignedConsumerInput::PositionResolutionFailed {
            fence: *fence,
            now: Moment::from_tick(1),
            failure: PositionResolutionAttemptFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("fail symbolic position: {error}"));
    machine
        .pause_partitions(epoch, &[target])
        .unwrap_or_else(|error| panic!("pause failed symbolic position: {error}"));

    assert_eq!(
        machine.resume_retained_partitions(
            epoch,
            &[target],
            Moment::from_tick(2),
            Deadline::from_tick(100),
        ),
        Err(AssignedConsumerMachineError::PositionNotRetained { partition: target })
    );

    seek_while_paused(
        &mut machine,
        epoch,
        target,
        StartPosition::Offset(offset(9)),
    );
    let resumed = machine
        .resume_retained_partitions(
            epoch,
            &[target],
            Moment::from_tick(3),
            Deadline::from_tick(100),
        )
        .unwrap_or_else(|error| panic!("resume replacement position: {error}"));
    assert!(matches!(
        resumed.effects(),
        [AssignedConsumerEffect::FetchReady { next_offset, .. }]
            if *next_offset == offset(9)
    ));
}

fn paused_position_throttle() -> (
    AssignedConsumerMachine,
    super::super::AssignmentEpoch,
    super::super::AssignedTopicPartition,
    super::super::PositionFence,
) {
    let mut machine = AssignedConsumerMachine::new();
    let target = partition(2, 0);
    let transition = assign(&mut machine, vec![assigned(2, 0, StartPosition::Beginning)]);
    let epoch = transition
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assignment epoch"));
    let [AssignedConsumerEffect::ResolvePosition { fence, .. }] = transition.effects() else {
        panic!("beginning position must start one resolution");
    };
    machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence: *fence,
            next_offset: offset(8),
            now: Moment::from_tick(10),
            throttle_ticks: 5,
        })
        .unwrap_or_else(|error| panic!("install position throttle: {error}"));
    let paused = machine
        .pause_partitions(epoch, &[target])
        .unwrap_or_else(|error| panic!("pause position throttle: {error}"));
    let [AssignedConsumerEffect::Suspend { fence }] = paused.effects() else {
        panic!("pause should fence the retained position throttle");
    };
    (machine, epoch, target, *fence)
}

fn seek_while_paused(
    machine: &mut AssignedConsumerMachine,
    epoch: super::super::AssignmentEpoch,
    target: super::super::AssignedTopicPartition,
    position: StartPosition,
) -> super::super::PositionFence {
    let transition = machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: epoch,
            partition: target,
            position,
            now: Moment::from_tick(10),
            resolution_deadline: Deadline::from_tick(50),
        })
        .unwrap_or_else(|error| panic!("seek paused partition: {error}"));
    let [AssignedConsumerEffect::Suspend { fence }] = transition.effects() else {
        panic!("paused seek should only publish its replacement fence");
    };
    *fence
}

fn assigned_pair() -> (AssignedConsumerMachine, super::super::AssignmentEpoch) {
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
    (machine, epoch)
}
