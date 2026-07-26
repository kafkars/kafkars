//! Local-empty, ordered-success, missing-offset, and correlation scenarios.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    Moment, NextFetchOffset, PartitionIndex, TopicId,
};

use super::{
    GroupPositionBatch, GroupPositionBootstrapEffect, GroupPositionBootstrapInput,
    GroupPositionBootstrapMachine, GroupPositionBootstrapMachineError, GroupPositionBootstrapState,
    GroupPositionBootstrapTerminal, GroupPositionFence, GroupPositionPartitionFact,
    GroupPositionPartitionResult,
};

#[test]
fn empty_assignment_completes_locally_without_offset_fetch() {
    let fence = position_fence(2);
    let mut machine = machine(20, Vec::new());
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("empty start: {error}"));

    let Some(GroupPositionBootstrapEffect::Complete {
        fence: completed_fence,
        deadline,
        terminal: GroupPositionBootstrapTerminal::Ready(batch),
    }) = transition.into_effect()
    else {
        panic!("empty assignment must complete locally");
    };
    assert_eq!(completed_fence, fence);
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(batch.throttle_time_ms(), 0);
    assert!(batch.facts().is_empty());
    assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
}

#[test]
fn one_original_deadline_and_exact_fence_cross_the_only_offset_fetch() {
    let fence = position_fence(2);
    let assignment = vec![assigned(3, 0), assigned(3, 2), assigned(5, 0)];
    let mut machine = machine(20, assignment.clone());
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"));

    assert_eq!(
        transition.into_effect(),
        Some(GroupPositionBootstrapEffect::FetchOffsets {
            fence,
            deadline: Deadline::from_tick(20),
            partitions: assignment,
        })
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::AwaitingDriver);
    assert_eq!(
        machine
            .apply(GroupPositionBootstrapInput::DriverAccepted { fence })
            .unwrap_or_else(|error| panic!("driver acceptance: {error}"))
            .into_effect(),
        None
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::Submitted);
}

#[test]
fn invalid_lifecycle_fact_returns_losslessly_without_mutation() {
    let fence = position_fence(2);
    let assignment = vec![assigned(3, 0), assigned(3, 2), assigned(5, 0)];
    let mut machine = machine(20, assignment.clone());
    let result = machine.apply(GroupPositionBootstrapInput::DriverAccepted { fence });
    let error = match result {
        Ok(_) => panic!("driver acceptance before start must be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error.kind(),
        GroupPositionBootstrapMachineError::InvalidState
    );
    assert_eq!(
        error.input(),
        &GroupPositionBootstrapInput::DriverAccepted { fence }
    );
    assert_eq!(
        error.into_input(),
        GroupPositionBootstrapInput::DriverAccepted { fence }
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::Ready);
    assert_eq!(machine.partitions(), assignment);
}

#[test]
fn committed_offsets_and_throttle_settle_ready_in_assignment_order() {
    let fence = position_fence(2);
    let mut machine = submitted_machine(20);
    let facts = vec![
        committed(3, 0, 11),
        committed(3, 2, 19),
        committed(5, 0, 23),
    ];
    let transition = machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(9),
            batch: GroupPositionBatch::new(31, facts),
        })
        .unwrap_or_else(|error| panic!("offset response: {error}"));

    let Some(GroupPositionBootstrapEffect::Complete {
        terminal: GroupPositionBootstrapTerminal::Ready(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("all committed offsets must settle ready");
    };
    assert_eq!(batch.throttle_time_ms(), 31);
    assert_eq!(batch.facts()[2].partition(), assigned(5, 0));
    assert_eq!(
        batch.facts()[2].result(),
        GroupPositionPartitionResult::Committed(offset(23))
    );
    assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
}

#[test]
fn missing_offsets_under_error_fail_the_complete_assignment_atomically() {
    let fence = position_fence(2);
    let mut machine = submitted_machine(20);
    let transition = machine
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(9),
            batch: GroupPositionBatch::new(
                37,
                vec![
                    committed(3, 0, 11),
                    GroupPositionPartitionFact::missing(assigned(3, 2)),
                    committed(5, 0, 23),
                ],
            ),
        })
        .unwrap_or_else(|error| panic!("missing response: {error}"));

    let Some(GroupPositionBootstrapEffect::Complete {
        terminal: GroupPositionBootstrapTerminal::MissingOffsets(missing),
        ..
    }) = transition.into_effect()
    else {
        panic!("Error policy must reject every partition atomically");
    };
    assert_eq!(missing.batch().throttle_time_ms(), 37);
    assert_eq!(missing.batch().facts().len(), 3);
    assert_eq!(missing.first_missing().partition(), assigned(3, 2));
    assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
}

#[test]
fn malformed_response_correlation_fails_terminally_without_activation() {
    for facts in [
        vec![committed(3, 0, 11)],
        vec![
            committed(3, 2, 19),
            committed(3, 0, 11),
            committed(5, 0, 23),
        ],
    ] {
        let mut machine = submitted_machine(20);
        let terminal = machine
            .apply(GroupPositionBootstrapInput::OffsetsFetched {
                fence: position_fence(2),
                now: Moment::from_tick(9),
                batch: GroupPositionBatch::new(0, facts),
            })
            .unwrap_or_else(|error| panic!("invalid correlation terminal: {error}"));
        assert!(matches!(
            terminal.into_effect(),
            Some(GroupPositionBootstrapEffect::Complete {
                terminal: GroupPositionBootstrapTerminal::Failed(failure),
                ..
            }) if failure.kind() == super::GroupPositionBootstrapFailureKind::InvalidResponse
        ));
        assert_eq!(machine.state(), GroupPositionBootstrapState::Completed);
    }
}

fn submitted_machine(deadline: u64) -> GroupPositionBootstrapMachine {
    let fence = position_fence(2);
    let mut machine = machine(
        deadline,
        vec![assigned(3, 0), assigned(3, 2), assigned(5, 0)],
    );
    machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(GroupPositionBootstrapInput::DriverAccepted { fence }))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn machine(
    deadline: u64,
    partitions: Vec<GroupAssignmentPartition>,
) -> GroupPositionBootstrapMachine {
    GroupPositionBootstrapMachine::try_new(
        position_fence(2),
        Deadline::from_tick(deadline),
        partitions,
    )
    .unwrap_or_else(|error| panic!("valid machine: {error}"))
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

fn committed(topic: u64, partition: u32, next_offset: i64) -> GroupPositionPartitionFact {
    GroupPositionPartitionFact::committed(assigned(topic, partition), offset(next_offset))
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
