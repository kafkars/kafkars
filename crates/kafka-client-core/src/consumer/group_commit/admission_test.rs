//! Local assignment fencing, allocation, and accepted-obligation evidence.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitAdmission, GroupOffsetCommitAdmissionError,
    GroupOffsetCommitAdmissionErrorKind, GroupOffsetCommitEffect, GroupOffsetCommitInput,
    GroupOffsetCommitMachine, GroupOffsetCommitPartitionOutcome, GroupOffsetCommitState,
    GroupOffsetCommitTerminal, LiveGroupAssignment, MemberId, OperationId, PartitionIndex, TopicId,
};

use super::assignment::reserve_expected_partitions;

#[test]
fn admission_emits_the_only_submit_with_original_identity_deadline_and_checkpoint() {
    let assignment = assignment(1, 2, 3, &[(5, 0), (5, 2)]);
    let admission = GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(23),
        Deadline::from_tick(29),
        Some(&assignment),
        checkpoint(1, 2, 3, &[(5, 0, 11), (5, 2, 19)]),
    )
    .unwrap_or_else(|error| panic!("live checkpoint should admit: {error}"));
    let (machine, submit) = admission.into_parts();
    let GroupOffsetCommitEffect::Submit {
        operation_id,
        deadline,
        checkpoint,
    } = submit
    else {
        panic!("admission must emit submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, Deadline::from_tick(29));
    assert_eq!(checkpoint.entries()[0].next_offset(), 11);
    assert_eq!(machine.operation_id(), operation_id);
    assert_eq!(machine.deadline(), deadline);
    assert_eq!(machine.state(), GroupOffsetCommitState::AwaitingDriver);
    assert!(machine.expected_capacity() >= 2);
}

#[test]
fn stale_lost_or_unassigned_checkpoints_reject_locally_and_remain_linear() {
    let live = assignment(1, 2, 3, &[(5, 0)]);
    for (candidate, live_assignment, expected) in [
        (
            checkpoint(9, 2, 3, &[(5, 0, 1)]),
            Some(&live),
            GroupOffsetCommitAdmissionErrorKind::GroupMismatch,
        ),
        (
            checkpoint(1, 9, 3, &[(5, 0, 1)]),
            Some(&live),
            GroupOffsetCommitAdmissionErrorKind::MemberMismatch,
        ),
        (
            checkpoint(1, 2, 9, &[(5, 0, 1)]),
            Some(&live),
            GroupOffsetCommitAdmissionErrorKind::GenerationMismatch,
        ),
        (
            checkpoint(1, 2, 3, &[(5, 1, 1)]),
            Some(&live),
            GroupOffsetCommitAdmissionErrorKind::UnassignedPartition {
                topic_id: topic(5),
                partition: partition(1),
            },
        ),
    ] {
        let error = rejected(GroupOffsetCommitMachine::try_admit(
            OperationId::from_raw(1),
            Deadline::from_tick(10),
            live_assignment,
            candidate,
        ));
        assert_eq!(error.kind(), expected);
        assert_eq!(error.checkpoint().entries().len(), 1);
        assert_eq!(error.into_checkpoint().entries()[0].next_offset(), 1);
    }

    let lost = rejected(GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        None,
        checkpoint(1, 2, 3, &[(5, 0, 1)]),
    ));
    assert_eq!(
        lost.kind(),
        GroupOffsetCommitAdmissionErrorKind::AssignmentLost
    );
}

#[test]
fn allocation_failure_is_explicit_and_recovers_the_exact_linear_checkpoint() {
    let mut expected = Vec::new();
    assert!(!reserve_expected_partitions(&mut expected, usize::MAX));
    assert!(expected.is_empty());

    let error = GroupOffsetCommitAdmissionError::new(
        GroupOffsetCommitAdmissionErrorKind::AllocationFailed,
        checkpoint(1, 2, 3, &[(5, 0, 17)]),
    );
    assert_eq!(
        error.kind(),
        GroupOffsetCommitAdmissionErrorKind::AllocationFailed
    );
    let recovered = error.into_checkpoint();
    assert_eq!(recovered.group_id(), group(1));
    assert_eq!(recovered.entries()[0].next_offset(), 17);
}

#[test]
fn later_assignment_loss_does_not_erase_an_admitted_terminal_obligation() {
    let live = assignment(1, 2, 3, &[(5, 0)]);
    let admission = GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(4),
        Deadline::from_tick(20),
        Some(&live),
        checkpoint(1, 2, 3, &[(5, 0, 8)]),
    )
    .unwrap_or_else(|error| panic!("live checkpoint: {error}"));
    let (mut machine, _submit) = admission.into_parts();
    drop(live);

    machine
        .apply(GroupOffsetCommitInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    let transition = machine
        .apply(GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 13,
            outcomes: vec![GroupOffsetCommitPartitionOutcome::committed(
                topic(5),
                partition(0),
            )],
        })
        .unwrap_or_else(|error| panic!("terminal response: {error}"));
    let Some(GroupOffsetCommitEffect::Complete {
        operation_id,
        terminal: GroupOffsetCommitTerminal::Committed(batch),
    }) = transition.into_effect()
    else {
        panic!("accepted operation must complete");
    };
    assert_eq!(operation_id, OperationId::from_raw(4));
    assert_eq!(batch.throttle_time_ms(), 13);
    assert_eq!(machine.state(), GroupOffsetCommitState::Completed);

    let replacement = assignment(1, 2, 4, &[(5, 0)]);
    let stale = rejected(GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(5),
        Deadline::from_tick(30),
        Some(&replacement),
        checkpoint(1, 2, 3, &[(5, 0, 9)]),
    ));
    assert_eq!(
        stale.kind(),
        GroupOffsetCommitAdmissionErrorKind::GenerationMismatch
    );
}

fn rejected(
    result: Result<GroupOffsetCommitAdmission, GroupOffsetCommitAdmissionError>,
) -> GroupOffsetCommitAdmissionError {
    match result {
        Ok(_) => panic!("checkpoint should reject"),
        Err(error) => error,
    }
}

fn assignment(
    group_id: u64,
    member_id: u64,
    assignment_generation: u64,
    partitions: &[(u64, u32)],
) -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        group(group_id),
        member(member_id),
        generation(assignment_generation),
        partitions
            .iter()
            .map(|&(topic_id, partition_index)| {
                GroupAssignmentPartition::new(topic(topic_id), partition(partition_index))
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("valid assignment: {error}"))
}

fn checkpoint(
    group_id: u64,
    member_id: u64,
    assignment_generation: u64,
    entries: &[(u64, u32, i64)],
) -> GroupCheckpoint {
    GroupCheckpoint::try_new(
        group(group_id),
        member(member_id),
        generation(assignment_generation),
        entries
            .iter()
            .map(|&(topic_id, partition_index, next_offset)| {
                GroupCheckpointEntry::try_new(
                    topic(topic_id),
                    partition(partition_index),
                    next_offset,
                    None,
                )
                .unwrap_or_else(|error| panic!("valid checkpoint entry: {error}"))
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("valid checkpoint: {error}"))
}

fn group(value: u64) -> GroupId {
    GroupId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero group"))
}

fn member(value: u64) -> MemberId {
    MemberId::try_from_raw(value).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value).unwrap_or_else(|| panic!("nonzero generation"))
}

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}

const fn partition(value: u32) -> PartitionIndex {
    PartitionIndex::from_raw(value)
}
