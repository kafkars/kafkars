//! Bounded call admission and shared classic-group commit call fixtures.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitEffect, GroupOffsetCommitInput,
    GroupOffsetCommitMachine, LiveGroupAssignment, MemberId, OperationId, PartitionIndex, TopicId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    protocol::consumer::{
        ClassicGroupCommitSession, GroupOffsetCommitEntryReservation,
        GroupOffsetCommitResultReservation, GroupOffsetCommitTopicName, PreparedGroupOffsetCommit,
        PreparedGroupOffsetCommitRequest, group_offset_commit_request,
    },
};

use super::{super::DriverOwner, group_offset_commit_calls::TrackedGroupOffsetCommitCalls};

#[test]
fn exactly_eight_accepted_calls_occupy_the_configured_first_lane() {
    let mut owner = owner();
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    for operation in 1..=8 {
        let permit = calls
            .try_reserve_group_commit()
            .unwrap_or_else(|| panic!("slot {operation} must be available"));
        let prepared = prepared(operation);
        let request = PreparedGroupOffsetCommitRequest::from_request_for_test(
            group_offset_commit_request(&prepared),
        );
        assert_eq!(
            permit
                .submit_prebuilt(&owner, prepared, request)
                .unwrap_or_else(|_| panic!("driver accepts call {operation}")),
            GroupOffsetCommitInput::DriverAccepted
        );
    }
    assert_eq!(calls.retained_group_commit_count(), 8);
    assert!(calls.try_reserve_group_commit().is_none());
    owner
        .shutdown_with_turn_limit(128, Duration::from_millis(5))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    let recovery = calls.recover_group_commits_after_driver_shutdown();
    let (recovered, settled, pending, completion) = recovery.into_parts();
    assert_eq!(recovered.len(), 8);
    assert_eq!(recovered[0].operation_id(), OperationId::from_raw(1));
    assert_eq!(recovered[7].operation_id(), OperationId::from_raw(8));
    assert!(settled.is_none());
    assert_eq!(pending, None);
    assert!(completion.is_none());
    assert_eq!(calls.retained_group_commit_count(), 0);
}

#[test]
fn driver_admission_failure_recovers_prepared_before_core_rejection() {
    let mut owner = owner();
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(5))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    let mut calls = TrackedGroupOffsetCommitCalls::new(8);
    let permit = calls
        .try_reserve_group_commit()
        .unwrap_or_else(|| panic!("one slot"));
    let prepared = prepared(12);
    let request = PreparedGroupOffsetCommitRequest::from_request_for_test(
        group_offset_commit_request(&prepared),
    );
    let failure = result_error(
        permit.submit_prebuilt(&owner, prepared, request),
        "closed driver rejects definitely unsent",
    );
    let (prepared, input, source) = failure.into_parts();
    assert_eq!(prepared.operation_id(), OperationId::from_raw(12));
    assert_eq!(
        prepared.operation_deadline().core(),
        Deadline::from_tick(100)
    );
    assert_eq!(prepared.entries_capacity(), 1);
    let mut machine = awaiting_machine(OperationId::from_raw(12));
    let transition = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("core accepts driver rejection: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(GroupOffsetCommitEffect::Complete { .. })
    ));
    assert!(matches!(
        source,
        super::group_offset_commit_submission::GroupOffsetCommitSubmitError::Driver(
            kafka_driver::SubmitError::Closed
        )
    ));
    assert_eq!(calls.retained_group_commit_count(), 0);
}

pub(super) fn prepared(operation: u64) -> PreparedGroupOffsetCommit {
    let entry_reservation = GroupOffsetCommitEntryReservation::try_new(1)
        .unwrap_or_else(|error| panic!("reserve entry capacity: {error:?}"));
    let result_reservation = GroupOffsetCommitResultReservation::try_new(1)
        .unwrap_or_else(|error| panic!("reserve result capacity: {error:?}"));
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(100),
        Instant::now() + Duration::from_secs(1),
    );
    let checkpoint = GroupCheckpoint::try_new(
        group_id(),
        member_id(),
        generation(),
        vec![
            GroupCheckpointEntry::try_new(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
                10,
                None,
            )
            .unwrap_or_else(|error| panic!("valid entry: {error}")),
        ],
    )
    .unwrap_or_else(|error| panic!("valid checkpoint: {error}"));
    PreparedGroupOffsetCommit::from_effect(
        GroupOffsetCommitEffect::Submit {
            operation_id: OperationId::from_raw(operation),
            deadline: deadline.core(),
            checkpoint,
        },
        deadline,
        ClassicGroupCommitSession::new(
            group_id(),
            Arc::from("readers"),
            member_id(),
            Arc::from("member-a"),
            generation(),
            4,
        ),
        vec![GroupOffsetCommitTopicName::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
        )],
        entry_reservation,
        result_reservation,
    )
    .unwrap_or_else(|error| panic!("valid prepared commit: {:?}", error.kind()))
}

pub(super) fn awaiting_machine(operation_id: OperationId) -> GroupOffsetCommitMachine {
    let checkpoint = GroupCheckpoint::try_new(
        group_id(),
        member_id(),
        generation(),
        vec![
            GroupCheckpointEntry::try_new(
                TopicId::from_raw(1),
                PartitionIndex::from_raw(0),
                10,
                None,
            )
            .unwrap_or_else(|error| panic!("valid machine entry: {error}")),
        ],
    )
    .unwrap_or_else(|error| panic!("valid machine checkpoint: {error}"));
    let assignment = LiveGroupAssignment::try_new(
        group_id(),
        member_id(),
        generation(),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("valid live assignment: {error}"));
    let admission = GroupOffsetCommitMachine::try_admit(
        operation_id,
        Deadline::from_tick(100),
        Some(&assignment),
        checkpoint,
    )
    .unwrap_or_else(|error| panic!("admit machine: {error}"));
    let (machine, _submit) = admission.into_parts();
    machine
}

pub(super) fn result_error<T, E>(result: Result<T, E>, context: &str) -> E {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build driver owner: {error}"))
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"))
}

fn member_id() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(4).unwrap_or_else(|| panic!("nonzero generation"))
}
