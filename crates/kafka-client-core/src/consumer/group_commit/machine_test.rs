//! Exact correlation, deadline, certainty, and terminal ownership scenarios.

use core::num::NonZeroI16;

use crate::{
    AssignmentGeneration, Deadline, DeliveryStatus, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitBrokerError, GroupOffsetCommitEffect,
    GroupOffsetCommitFailureKind, GroupOffsetCommitInput, GroupOffsetCommitMachine,
    GroupOffsetCommitMachineError, GroupOffsetCommitPartitionOutcome,
    GroupOffsetCommitPartitionResult, GroupOffsetCommitState, GroupOffsetCommitTerminal,
    GroupOffsetCommitTransition, LiveGroupAssignment, MemberId, OperationId, PartitionIndex,
    TopicId,
};

#[test]
fn mixed_response_is_broker_rejected_by_first_error_and_retains_partial_success() {
    let mut machine = submitted_machine();
    let first_code = NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero code"));
    let later_code = NonZeroI16::new(71).unwrap_or_else(|| panic!("nonzero code"));
    let outcomes = vec![
        GroupOffsetCommitPartitionOutcome::committed(topic(5), partition(0)),
        GroupOffsetCommitPartitionOutcome::rejected(
            topic(5),
            partition(2),
            GroupOffsetCommitBrokerError::new(first_code),
        ),
        GroupOffsetCommitPartitionOutcome::rejected(
            topic(6),
            partition(0),
            GroupOffsetCommitBrokerError::new(later_code),
        ),
    ];
    let transition = machine
        .apply(GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 31,
            outcomes,
        })
        .unwrap_or_else(|error| panic!("correlated response: {error}"));
    let Some(GroupOffsetCommitEffect::Complete {
        terminal: GroupOffsetCommitTerminal::BrokerRejected(rejection),
        ..
    }) = transition.into_effect()
    else {
        panic!("correlated response must complete");
    };

    assert_eq!(rejection.batch().throttle_time_ms(), 31);
    assert_eq!(rejection.batch().outcomes().len(), 3);
    let GroupOffsetCommitPartitionResult::Rejected(error) = rejection.first_rejected().result()
    else {
        panic!("second partition should retain rejection");
    };
    assert_eq!(error.code(), -32_000);
    let GroupOffsetCommitPartitionResult::Rejected(later) =
        rejection.batch().outcomes()[2].result()
    else {
        panic!("later rejection should remain available");
    };
    assert_eq!(later.code(), 71);
    let Err(error) = machine.apply(GroupOffsetCommitInput::TransportFailed {
        delivery: DeliveryStatus::NotSent,
    }) else {
        panic!("terminal machine must reject another fact");
    };
    assert_eq!(
        error.kind(),
        GroupOffsetCommitMachineError::AlreadyCompleted
    );
}

#[test]
fn malformed_broker_correlation_settles_once_as_possibly_sent() {
    for outcomes in [
        vec![GroupOffsetCommitPartitionOutcome::committed(
            topic(5),
            partition(0),
        )],
        vec![
            GroupOffsetCommitPartitionOutcome::committed(topic(5), partition(2)),
            GroupOffsetCommitPartitionOutcome::committed(topic(5), partition(0)),
        ],
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(GroupOffsetCommitInput::BrokerResponded {
                    throttle_time_ms: 0,
                    outcomes,
                })
                .unwrap_or_else(|error| panic!("invalid response must settle: {error}")),
            GroupOffsetCommitFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), GroupOffsetCommitState::Completed);
    }
}

#[test]
fn pre_driver_failures_are_not_sent_and_assign_one_terminal() {
    for (input, kind) in [
        (
            GroupOffsetCommitInput::DriverRejected,
            GroupOffsetCommitFailureKind::DriverRejected,
        ),
        (
            GroupOffsetCommitInput::DeadlineElapsed {
                delivery: DeliveryStatus::NotSent,
            },
            GroupOffsetCommitFailureKind::DeadlineElapsed,
        ),
    ] {
        let (mut machine, _submit) = admitted_machine();
        assert_failure(
            machine
                .apply(input)
                .unwrap_or_else(|error| panic!("pre-driver failure: {error}")),
            kind,
            DeliveryStatus::NotSent,
        );
        let Err(error) = machine.apply(GroupOffsetCommitInput::DriverAccepted) else {
            panic!("terminal machine must reject driver acceptance");
        };
        assert_eq!(
            error.kind(),
            GroupOffsetCommitMachineError::AlreadyCompleted
        );
    }
}

#[test]
fn deadline_after_driver_acceptance_preserves_timeout_kind_and_certainty() {
    for delivery in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(GroupOffsetCommitInput::DeadlineElapsed { delivery })
                .unwrap_or_else(|error| panic!("driver deadline terminal: {error}")),
            GroupOffsetCommitFailureKind::DeadlineElapsed,
            delivery,
        );
    }
}

#[test]
fn submitted_nontransport_failures_keep_exact_categories_and_certainty() {
    for (input, kind, delivery) in [
        (
            GroupOffsetCommitInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            GroupOffsetCommitFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            GroupOffsetCommitInput::ResponseTooLarge,
            GroupOffsetCommitFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            GroupOffsetCommitInput::InvalidResponse,
            GroupOffsetCommitFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(input)
                .unwrap_or_else(|error| panic!("exact terminal category: {error}")),
            kind,
            delivery,
        );
    }
}

#[test]
fn transport_terminal_preserves_driver_certainty_without_retry() {
    for delivery in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(GroupOffsetCommitInput::TransportFailed { delivery })
                .unwrap_or_else(|error| panic!("transport terminal: {error}")),
            GroupOffsetCommitFailureKind::Transport,
            delivery,
        );
        assert_eq!(machine.state(), GroupOffsetCommitState::Completed);
    }
}

fn admitted_machine() -> (GroupOffsetCommitMachine, GroupOffsetCommitEffect) {
    GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(23),
        Deadline::from_tick(29),
        Some(&assignment(1, 2, 3, &[(5, 0), (5, 2), (6, 0)])),
        checkpoint(1, 2, 3, &[(5, 0, 11), (5, 2, 19), (6, 0, 23)]),
    )
    .unwrap_or_else(|error| panic!("valid checkpoint: {error}"))
    .into_parts()
}

fn submitted_machine() -> GroupOffsetCommitMachine {
    let (mut machine, _submit) = admitted_machine();
    machine
        .apply(GroupOffsetCommitInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert!(machine.expected_capacity() >= 3);
    machine
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

fn assert_failure(
    transition: GroupOffsetCommitTransition,
    expected_kind: GroupOffsetCommitFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(GroupOffsetCommitEffect::Complete {
        terminal: GroupOffsetCommitTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
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
