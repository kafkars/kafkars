//! Deterministic tests for sequential missing-offset reset ownership.

use super::{
    GroupPositionBatch, GroupPositionFence, GroupPositionMissingOffsetReset,
    GroupPositionPartitionFact, GroupPositionPartitionResult, GroupPositionResetApplyError,
    GroupPositionResetEffect, GroupPositionResetInput, GroupPositionResetMachine,
    GroupPositionResetMachineError, GroupPositionResetState, GroupPositionResetTerminal,
    GroupPositionResetTransition,
};
use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    Moment, NextFetchOffset, PartitionIndex, PositionResolutionAttemptFailure, StartPosition,
    TopicId,
};

#[test]
fn missing_partitions_resolve_sequentially_in_assignment_order() {
    let fence = fence();
    let deadline = Deadline::from_tick(100);
    let mut machine = machine(fence, deadline);

    let first = applied(
        machine.apply(GroupPositionResetInput::Start {
            fence,
            now: Moment::from_tick(1),
        }),
        "start",
    );
    assert_resolve(first, fence, deadline, partition(0));
    assert_eq!(machine.state(), GroupPositionResetState::AwaitingDriver);

    assert_eq!(
        applied(
            machine.apply(GroupPositionResetInput::DriverAccepted {
                fence,
                partition: partition(0),
            }),
            "accept",
        )
        .into_effect(),
        None
    );
    let second = applied(
        machine.apply(GroupPositionResetInput::OffsetResolved {
            fence,
            partition: partition(0),
            now: Moment::from_tick(2),
            next_offset: offset(7),
            throttle_time_ms: 4,
        }),
        "first resolution",
    );
    assert_resolve(second, fence, deadline, partition(2));

    applied(
        machine.apply(GroupPositionResetInput::DriverAccepted {
            fence,
            partition: partition(2),
        }),
        "second accept",
    );
    let terminal = applied(
        machine.apply(GroupPositionResetInput::OffsetResolved {
            fence,
            partition: partition(2),
            now: Moment::from_tick(3),
            next_offset: offset(11),
            throttle_time_ms: 9,
        }),
        "second resolution",
    )
    .into_effect();
    let Some(terminal) = terminal else {
        panic!("complete");
    };
    let GroupPositionResetEffect::Complete {
        fence: effect_fence,
        deadline: effect_deadline,
        terminal: GroupPositionResetTerminal::Ready(batch),
    } = terminal
    else {
        panic!("expected ready terminal");
    };
    assert_eq!(effect_fence, fence);
    assert_eq!(effect_deadline, deadline);
    assert_eq!(batch.throttle_time_ms(), 9);
    assert_eq!(
        batch.facts(),
        &[
            GroupPositionPartitionFact::committed(partition(0), offset(7)),
            GroupPositionPartitionFact::committed(partition(1), offset(5)),
            GroupPositionPartitionFact::committed(partition(2), offset(11)),
        ]
    );
    assert_eq!(machine.state(), GroupPositionResetState::Completed);
}

#[test]
fn original_deadline_precedes_late_success() {
    let fence = fence();
    let mut machine = machine(fence, Deadline::from_tick(10));
    applied(
        machine.apply(GroupPositionResetInput::Start {
            fence,
            now: Moment::from_tick(1),
        }),
        "start",
    );
    applied(
        machine.apply(GroupPositionResetInput::DriverAccepted {
            fence,
            partition: partition(0),
        }),
        "accept",
    );

    let effect = applied(
        machine.apply(GroupPositionResetInput::OffsetResolved {
            fence,
            partition: partition(0),
            now: Moment::from_tick(10),
            next_offset: offset(99),
            throttle_time_ms: 0,
        }),
        "late terminal",
    )
    .into_effect();
    let Some(effect) = effect else {
        panic!("complete");
    };
    let GroupPositionResetEffect::Complete {
        terminal: GroupPositionResetTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(
        failure.failure(),
        PositionResolutionAttemptFailure::DeadlineElapsed
    );
    assert_eq!(failure.partition(), partition(0));
    assert_eq!(
        failure.batch().facts()[0].result(),
        GroupPositionPartitionResult::Missing
    );
}

#[test]
fn stale_partition_is_returned_without_mutation() {
    let fence = fence();
    let mut machine = machine(fence, Deadline::from_tick(100));
    applied(
        machine.apply(GroupPositionResetInput::Start {
            fence,
            now: Moment::from_tick(1),
        }),
        "start",
    );
    let input = GroupPositionResetInput::DriverAccepted {
        fence,
        partition: partition(2),
    };
    let Err(error) = machine.apply(input) else {
        panic!("stale partition");
    };
    assert_eq!(error.kind(), GroupPositionResetMachineError::StalePartition);
    assert_eq!(error.into_input(), input);
    assert_eq!(machine.current_partition(), Some(partition(0)));
    assert_eq!(machine.state(), GroupPositionResetState::AwaitingDriver);
}

fn machine(fence: GroupPositionFence, deadline: Deadline) -> GroupPositionResetMachine {
    GroupPositionResetMachine::new(
        fence,
        deadline,
        GroupPositionMissingOffsetReset::new(
            GroupPositionBatch::new(
                3,
                vec![
                    GroupPositionPartitionFact::missing(partition(0)),
                    GroupPositionPartitionFact::committed(partition(1), offset(5)),
                    GroupPositionPartitionFact::missing(partition(2)),
                ],
            ),
            0,
            StartPosition::Beginning,
        ),
    )
}

fn assert_resolve(
    transition: GroupPositionResetTransition,
    fence: GroupPositionFence,
    deadline: Deadline,
    partition: GroupAssignmentPartition,
) {
    assert_eq!(
        transition.into_effect(),
        Some(GroupPositionResetEffect::ResolveOffset {
            fence,
            deadline,
            partition,
            position: StartPosition::Beginning,
        })
    );
}

fn fence() -> GroupPositionFence {
    let Some(group) = GroupId::try_from_raw(1) else {
        panic!("group");
    };
    let Some(cycle) = MembershipCycle::try_from_raw(2) else {
        panic!("cycle");
    };
    let Some(member) = MemberId::try_from_raw(3) else {
        panic!("member");
    };
    let Some(generation) = AssignmentGeneration::try_from_raw(4) else {
        panic!("generation");
    };
    GroupPositionFence::new(group, cycle, member, generation)
}

fn partition(index: usize) -> GroupAssignmentPartition {
    let Ok(index) = u32::try_from(index) else {
        panic!("partition");
    };
    GroupAssignmentPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(index))
}

fn offset(raw: i64) -> NextFetchOffset {
    let Some(offset) = NextFetchOffset::try_from_raw(raw) else {
        panic!("offset");
    };
    offset
}

fn applied(
    result: Result<GroupPositionResetTransition, GroupPositionResetApplyError>,
    context: &str,
) -> GroupPositionResetTransition {
    match result {
        Ok(transition) => transition,
        Err(error) => panic!("{context}: {error:?}"),
    }
}
