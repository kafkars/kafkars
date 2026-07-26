//! Stale-fence, deadline-precedence, and exact terminal failure scenarios.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    Moment, PartitionIndex, TopicId,
};

use super::{
    GroupPositionBatch, GroupPositionBootstrapEffect, GroupPositionBootstrapFailureKind,
    GroupPositionBootstrapFetchFailure, GroupPositionBootstrapInput, GroupPositionBootstrapMachine,
    GroupPositionBootstrapMachineError, GroupPositionBootstrapState,
    GroupPositionBootstrapTerminal, GroupPositionFence, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};

#[test]
fn stale_facts_return_losslessly_without_mutating_active_bootstrap() {
    let active = position_fence(2);
    let stale = position_fence(1);
    let mut machine = submitted_machine(20);
    let input = GroupPositionBootstrapInput::OffsetsFetched {
        fence: stale,
        now: Moment::from_tick(9),
        batch: GroupPositionBatch::new(
            17,
            vec![GroupPositionPartitionFact::missing(assigned(3, 0))],
        ),
    };
    let Err(error) = machine.apply(input) else {
        panic!("stale response must reject losslessly");
    };

    assert_eq!(error.kind(), GroupPositionBootstrapMachineError::StaleFence);
    assert_eq!(machine.fence(), active);
    let GroupPositionBootstrapInput::OffsetsFetched { batch, .. } = error.into_input() else {
        panic!("exact stale input must be returned");
    };
    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(
        batch.facts()[0].result(),
        GroupPositionPartitionResult::Missing
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::Submitted);
}

#[test]
fn original_deadline_precedes_late_success_and_terminalizes_once() {
    let fence = position_fence(2);
    let mut machine = submitted_machine(10);
    let transition = machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(10),
            batch: GroupPositionBatch::new(
                0,
                vec![GroupPositionPartitionFact::missing(assigned(3, 0))],
            ),
        })
        .unwrap_or_else(|error| panic!("late response must settle deadline: {error}"));

    assert_failure(
        transition,
        GroupPositionBootstrapFailureKind::DeadlineElapsed,
    );
    let Err(error) = machine.apply(GroupPositionBootstrapInput::DeadlineElapsed {
        fence,
        now: Moment::from_tick(11),
    }) else {
        panic!("completed machine must reject another terminal");
    };
    assert_eq!(
        error.kind(),
        GroupPositionBootstrapMachineError::AlreadyCompleted
    );
}

#[test]
fn early_deadline_fact_is_rejected_without_spending_terminal_ownership() {
    let fence = position_fence(2);
    let mut machine = submitted_machine(10);
    let Err(error) = machine.apply(GroupPositionBootstrapInput::DeadlineElapsed {
        fence,
        now: Moment::from_tick(9),
    }) else {
        panic!("early deadline must reject");
    };

    assert_eq!(
        error.kind(),
        GroupPositionBootstrapMachineError::DeadlineNotElapsed
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::Submitted);
}

#[test]
fn every_started_driver_failure_has_one_terminal_decision() {
    for (input, expected) in [
        (
            GroupPositionBootstrapInput::FetchFailed {
                fence: position_fence(2),
                now: Moment::from_tick(9),
                failure: GroupPositionBootstrapFetchFailure::Transport,
            },
            GroupPositionBootstrapFailureKind::Transport,
        ),
        (
            GroupPositionBootstrapInput::FetchFailed {
                fence: position_fence(2),
                now: Moment::from_tick(9),
                failure: GroupPositionBootstrapFetchFailure::Compatibility,
            },
            GroupPositionBootstrapFailureKind::Compatibility,
        ),
        (
            GroupPositionBootstrapInput::FetchFailed {
                fence: position_fence(2),
                now: Moment::from_tick(9),
                failure: GroupPositionBootstrapFetchFailure::InvalidResponse,
            },
            GroupPositionBootstrapFailureKind::InvalidResponse,
        ),
        (
            GroupPositionBootstrapInput::FetchFailed {
                fence: position_fence(2),
                now: Moment::from_tick(9),
                failure: GroupPositionBootstrapFetchFailure::ResponseTooLarge,
            },
            GroupPositionBootstrapFailureKind::ResponseTooLarge,
        ),
    ] {
        let mut machine = submitted_machine(20);
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("driver failure terminal: {error}"));
        assert_failure(transition, expected);
        assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
    }
}

#[test]
fn pre_driver_rejection_and_elapsed_start_complete_without_stranding() {
    let fence = position_fence(2);
    let mut rejected = started_machine(20);
    assert_failure(
        rejected
            .apply(GroupPositionBootstrapInput::DriverRejected {
                fence,
                now: Moment::from_tick(9),
            })
            .unwrap_or_else(|error| panic!("pre-driver rejection: {error}")),
        GroupPositionBootstrapFailureKind::DriverRejected,
    );

    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(GroupPositionBootstrapInput::Start {
                fence,
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start: {error}")),
        GroupPositionBootstrapFailureKind::DeadlineElapsed,
    );
}

fn machine(deadline: u64) -> GroupPositionBootstrapMachine {
    GroupPositionBootstrapMachine::try_new(
        position_fence(2),
        Deadline::from_tick(deadline),
        vec![assigned(3, 0)],
    )
    .unwrap_or_else(|error| panic!("valid machine: {error}"))
}

fn started_machine(deadline: u64) -> GroupPositionBootstrapMachine {
    let fence = position_fence(2);
    let mut machine = machine(deadline);
    machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    machine
}

fn submitted_machine(deadline: u64) -> GroupPositionBootstrapMachine {
    let fence = position_fence(2);
    let mut machine = started_machine(deadline);
    machine
        .apply(GroupPositionBootstrapInput::DriverAccepted { fence })
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn assert_failure(
    transition: super::GroupPositionBootstrapTransition,
    expected: GroupPositionBootstrapFailureKind,
) {
    assert!(matches!(
        transition.into_effect(),
        Some(GroupPositionBootstrapEffect::Complete {
            terminal: GroupPositionBootstrapTerminal::Failed(failure),
            ..
        }) if failure.kind() == expected
    ));
}

fn position_fence(assignment_generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(13).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(assignment_generation)
            .unwrap_or_else(|| panic!("generation")),
    )
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}
