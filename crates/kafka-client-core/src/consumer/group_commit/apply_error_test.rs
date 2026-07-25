//! Lossless rejected-input and pre-mutation validation scenarios.

use crate::{
    AssignmentGeneration, Deadline, DeliveryStatus, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitInput, GroupOffsetCommitMachine,
    GroupOffsetCommitMachineError, GroupOffsetCommitPartitionOutcome, GroupOffsetCommitState,
    LiveGroupAssignment, MemberId, OperationId, PartitionIndex, TopicId,
};

#[test]
fn impossible_pre_driver_delivery_is_recoverable_without_mutation() {
    let mut machine = admitted_machine();
    let operation_id = machine.operation_id();
    let deadline = machine.deadline();
    let expected_capacity = machine.expected_capacity();
    let input = GroupOffsetCommitInput::DeadlineElapsed {
        delivery: DeliveryStatus::PossiblySent,
    };

    let Err(error) = machine.apply(input) else {
        panic!("impossible pre-driver delivery must reject");
    };

    assert_eq!(
        error.kind(),
        GroupOffsetCommitMachineError::InvalidDeliveryStatus
    );
    assert_eq!(
        error.into_input(),
        GroupOffsetCommitInput::DeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_awaiting_unchanged(&machine, operation_id, deadline, expected_capacity);
}

#[test]
fn wrong_state_recovers_exact_broker_response_vector() {
    let mut machine = admitted_machine();
    let operation_id = machine.operation_id();
    let deadline = machine.deadline();
    let expected_capacity = machine.expected_capacity();
    let (input, outcome_capacity) = broker_response(11);

    let Err(error) = machine.apply(input) else {
        panic!("response before driver acceptance must reject");
    };

    assert_eq!(error.kind(), GroupOffsetCommitMachineError::InvalidState);
    assert_response(error.into_input(), outcome_capacity);
    assert_awaiting_unchanged(&machine, operation_id, deadline, expected_capacity);
}

#[test]
fn completed_machine_recovers_exact_broker_response_vector() {
    let mut machine = admitted_machine();
    machine
        .apply(GroupOffsetCommitInput::DriverRejected)
        .unwrap_or_else(|error| panic!("driver rejection should settle: {error}"));
    let operation_id = machine.operation_id();
    let deadline = machine.deadline();
    let expected_capacity = machine.expected_capacity();
    let (input, outcome_capacity) = broker_response(17);

    let Err(error) = machine.apply(input) else {
        panic!("response after completion must reject");
    };

    assert_eq!(
        error.kind(),
        GroupOffsetCommitMachineError::AlreadyCompleted
    );
    assert_response(error.into_input(), outcome_capacity);
    assert_eq!(machine.state(), GroupOffsetCommitState::Completed);
    assert_eq!(machine.operation_id(), operation_id);
    assert_eq!(machine.deadline(), deadline);
    assert_eq!(machine.expected_capacity(), expected_capacity);
}

fn broker_response(requested_capacity: usize) -> (GroupOffsetCommitInput, usize) {
    let mut outcomes = Vec::with_capacity(requested_capacity);
    outcomes.push(GroupOffsetCommitPartitionOutcome::committed(
        topic(5),
        partition(0),
    ));
    outcomes.push(GroupOffsetCommitPartitionOutcome::committed(
        topic(5),
        partition(2),
    ));
    let capacity = outcomes.capacity();
    (
        GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms: 37,
            outcomes,
        },
        capacity,
    )
}

fn assert_response(input: GroupOffsetCommitInput, expected_capacity: usize) {
    let GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms,
        outcomes,
    } = input
    else {
        panic!("exact broker response should be recovered");
    };
    assert_eq!(throttle_time_ms, 37);
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes.capacity(), expected_capacity);
    assert_eq!(
        outcomes,
        [
            GroupOffsetCommitPartitionOutcome::committed(topic(5), partition(0)),
            GroupOffsetCommitPartitionOutcome::committed(topic(5), partition(2)),
        ]
    );
}

fn assert_awaiting_unchanged(
    machine: &GroupOffsetCommitMachine,
    operation_id: OperationId,
    deadline: Deadline,
    expected_capacity: usize,
) {
    assert_eq!(machine.state(), GroupOffsetCommitState::AwaitingDriver);
    assert_eq!(machine.operation_id(), operation_id);
    assert_eq!(machine.deadline(), deadline);
    assert_eq!(machine.expected_capacity(), expected_capacity);
}

fn admitted_machine() -> GroupOffsetCommitMachine {
    let assignment = LiveGroupAssignment::try_new(
        group(1),
        member(2),
        generation(3),
        vec![
            GroupAssignmentPartition::new(topic(5), partition(0)),
            GroupAssignmentPartition::new(topic(5), partition(2)),
        ],
    )
    .unwrap_or_else(|error| panic!("valid assignment: {error}"));
    let checkpoint = GroupCheckpoint::try_new(
        group(1),
        member(2),
        generation(3),
        vec![checkpoint_entry(5, 0, 11), checkpoint_entry(5, 2, 19)],
    )
    .unwrap_or_else(|error| panic!("valid checkpoint: {error}"));
    GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(23),
        Deadline::from_tick(29),
        Some(&assignment),
        checkpoint,
    )
    .unwrap_or_else(|error| panic!("valid admission: {error}"))
    .into_parts()
    .0
}

fn checkpoint_entry(topic_id: u64, partition_index: u32, next_offset: i64) -> GroupCheckpointEntry {
    GroupCheckpointEntry::try_new(
        topic(topic_id),
        partition(partition_index),
        next_offset,
        None,
    )
    .unwrap_or_else(|error| panic!("valid checkpoint entry: {error}"))
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
