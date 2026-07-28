//! Stable generated-free commit outcomes and exact correlation evidence.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitBatch, GroupOffsetCommitBrokerError,
    GroupOffsetCommitEffect, GroupOffsetCommitInput, GroupOffsetCommitMachine,
    GroupOffsetCommitPartitionOutcome, GroupOffsetCommitTerminal, GroupPositionFence,
    LiveGroupAssignment, MemberId, MembershipCycle, OperationId, PartitionIndex, TopicId,
};

use super::{
    GroupConsumerCommitDeliveryStatus, GroupConsumerCommitFailureKind,
    GroupConsumerCommitObserverError, GroupConsumerCommitOutcome,
    GroupConsumerCommitPartitionResult, outcome::translate_terminal,
};
use crate::consumer::group_batch::GroupConsumerCheckpointObservation;

#[test]
fn committed_terminal_exposes_stable_topic_partition_and_throttle() {
    let terminal = GroupOffsetCommitTerminal::Committed(GroupOffsetCommitBatch::new(
        13,
        vec![GroupOffsetCommitPartitionOutcome::committed(
            TopicId::from_raw(5),
            PartitionIndex::from_raw(2),
        )],
    ));

    let outcome = translate_terminal(terminal, observation())
        .unwrap_or_else(|error| panic!("terminal translation: {error}"));
    let GroupConsumerCommitOutcome::Committed(batch) = outcome else {
        panic!("committed terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 13);
    assert_eq!(batch.outcomes().len(), 1);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[0].partition(), 2);
    assert_eq!(
        batch.outcomes()[0].result(),
        GroupConsumerCommitPartitionResult::Committed
    );
}

#[test]
fn broker_rejection_preserves_exact_signed_code_and_partition_correlation() {
    let code = NonZeroI16::new(-32123).unwrap_or_else(|| panic!("nonzero code"));
    let terminal = broker_terminal(GroupOffsetCommitPartitionOutcome::rejected(
        TopicId::from_raw(5),
        PartitionIndex::from_raw(2),
        GroupOffsetCommitBrokerError::new(code),
    ));

    let observation = observation();
    let checkpoint_identity = observation.storage_identity();
    let outcome = translate_terminal(terminal, observation)
        .unwrap_or_else(|error| panic!("terminal translation: {error}"));
    let GroupConsumerCommitOutcome::BrokerRejected(batch, checkpoint) = outcome else {
        panic!("broker rejection expected");
    };
    let GroupConsumerCommitPartitionResult::Rejected(error) = batch.outcomes()[0].result() else {
        panic!("rejected partition expected");
    };
    assert_eq!(error.code(), -32123);
    assert_eq!(checkpoint.topic(), "orders");
    assert_eq!(checkpoint.partition(), 2);
    assert_eq!(checkpoint.next_offset(), 19);
    assert_eq!(checkpoint.storage_identity(), checkpoint_identity);
}

#[test]
fn whole_operation_failure_preserves_category_and_delivery_certainty() {
    let terminal = terminal_from_input(GroupOffsetCommitInput::DriverRejected);

    let outcome = translate_terminal(terminal, observation())
        .unwrap_or_else(|error| panic!("terminal translation: {error}"));
    let GroupConsumerCommitOutcome::Failed(failure, checkpoint) = outcome else {
        panic!("whole-operation failure expected");
    };
    assert_eq!(
        failure.kind(),
        GroupConsumerCommitFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        GroupConsumerCommitDeliveryStatus::NotSent
    );
    assert_eq!(checkpoint.next_offset(), 19);
}

#[test]
fn impossible_terminal_identity_is_an_observer_invariant_error() {
    let terminal = GroupOffsetCommitTerminal::Committed(GroupOffsetCommitBatch::new(
        0,
        vec![GroupOffsetCommitPartitionOutcome::committed(
            TopicId::from_raw(9),
            PartitionIndex::from_raw(2),
        )],
    ));

    assert!(matches!(
        translate_terminal(terminal, observation()),
        Err(GroupConsumerCommitObserverError::InternalInvariant)
    ));
}

fn broker_terminal(outcome: GroupOffsetCommitPartitionOutcome) -> GroupOffsetCommitTerminal {
    terminal_from_input(GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms: 7,
        outcomes: vec![outcome],
    })
}

fn terminal_from_input(input: GroupOffsetCommitInput) -> GroupOffsetCommitTerminal {
    let assignment = assignment();
    let (mut machine, _effect) = GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(1),
        Deadline::from_tick(10),
        Some(&assignment),
        checkpoint(),
    )
    .unwrap_or_else(|error| panic!("commit admission: {error}"))
    .into_parts();
    if !matches!(input, GroupOffsetCommitInput::DriverRejected) {
        machine
            .apply(GroupOffsetCommitInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    }
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("terminal input: {error}"));
    let Some(GroupOffsetCommitEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal effect expected");
    };
    terminal
}

fn assignment() -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        group(),
        member(),
        generation(),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(5),
            PartitionIndex::from_raw(2),
        )],
    )
    .unwrap_or_else(|error| panic!("live assignment: {error}"))
}

fn checkpoint() -> GroupCheckpoint {
    GroupCheckpoint::try_new(
        group(),
        member(),
        generation(),
        vec![
            GroupCheckpointEntry::try_new(
                TopicId::from_raw(5),
                PartitionIndex::from_raw(2),
                19,
                None,
            )
            .unwrap_or_else(|error| panic!("checkpoint entry: {error}")),
        ],
    )
    .unwrap_or_else(|error| panic!("checkpoint: {error}"))
}

fn observation() -> GroupConsumerCheckpointObservation {
    GroupConsumerCheckpointObservation::from_checkpoint(
        crate::consumer::GroupConsumerCheckpoint::from_test_parts(
            Arc::from("orders"),
            2,
            19,
            GroupPositionFence::new(group(), MembershipCycle::initial(), member(), generation()),
            checkpoint(),
        ),
    )
}

fn group() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"))
}

fn member() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(3).unwrap_or_else(|| panic!("nonzero generation"))
}
