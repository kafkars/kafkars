//! Prepared snapshot shape and shared classic-group commit test fixtures.

use std::{mem::size_of, sync::Arc};

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupCheckpoint, GroupCheckpointEntry, GroupId,
    GroupOffsetCommitEffect, GroupOffsetCommitPartitionOutcome, MemberId, OperationId,
    PartitionIndex, TopicId,
};

use crate::clock::OperationDeadline;

use super::{
    model::{PreparedGroupOffsetCommit, PreparedGroupOffsetCommitEntry},
    preparation::GroupOffsetCommitPreparationError,
    result_reservation::GroupOffsetCommitResultReservation,
    session::{ClassicGroupCommitSession, GroupOffsetCommitTopicName},
};

#[test]
fn successful_snapshot_retains_exact_capacity_order_and_leader_epoch_requirement() {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None), entry(1, 1, 20, Some(7))],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let prepared = prepare(effect, deadline, session, topics)
        .unwrap_or_else(|error| panic!("valid prepared commit: {:?}", error.kind()));
    assert_eq!(prepared.operation_id(), OperationId::from_raw(9));
    assert_eq!(prepared.entries_capacity(), 2);
    assert_eq!(prepared.outcomes_capacity(), 2);
    assert!(prepared.requires_leader_epoch());
    assert_eq!(prepared.entries()[0].next_offset(), 10);
    assert_eq!(prepared.entries()[1].leader_epoch(), Some(7));
    let expected_retained = "readers"
        .len()
        .checked_add("member-a".len())
        .and_then(|bytes| bytes.checked_add("orders".len()))
        .and_then(|bytes| {
            bytes.checked_add(2usize.saturating_mul(size_of::<PreparedGroupOffsetCommitEntry>()))
        })
        .and_then(|bytes| {
            bytes.checked_add(2usize.saturating_mul(size_of::<GroupOffsetCommitPartitionOutcome>()))
        });
    assert_eq!(prepared.retained_bytes(), expected_retained);
}

pub(super) fn prepared(
    entries: Vec<GroupCheckpointEntry>,
    classic_generation: i64,
    topics: Vec<GroupOffsetCommitTopicName>,
) -> PreparedGroupOffsetCommit {
    let (effect, deadline, session, topics) = inputs(
        entries,
        classic_generation,
        Arc::from("readers"),
        Arc::from("member-a"),
        topics,
    );
    prepare(effect, deadline, session, topics)
        .unwrap_or_else(|error| panic!("valid prepared commit: {:?}", error.kind()))
}

#[allow(
    clippy::result_large_err,
    reason = "test rejection scenarios must inspect every recovered linear owner"
)]
pub(super) fn prepare(
    effect: GroupOffsetCommitEffect,
    operation_deadline: OperationDeadline,
    session: ClassicGroupCommitSession,
    topics: Vec<GroupOffsetCommitTopicName>,
) -> Result<PreparedGroupOffsetCommit, GroupOffsetCommitPreparationError> {
    let entry_count = match &effect {
        GroupOffsetCommitEffect::Submit { checkpoint, .. } => checkpoint.entries().len(),
        GroupOffsetCommitEffect::Complete { .. } => 0,
    };
    PreparedGroupOffsetCommit::from_effect(
        effect,
        operation_deadline,
        session,
        topics,
        reservation(entry_count),
    )
}

pub(super) fn reservation(entry_count: usize) -> GroupOffsetCommitResultReservation {
    GroupOffsetCommitResultReservation::try_new(entry_count)
        .unwrap_or_else(|error| panic!("reserve exact test result capacity: {error:?}"))
}

pub(super) fn inputs(
    entries: Vec<GroupCheckpointEntry>,
    classic_generation: i64,
    group: Arc<str>,
    member: Arc<str>,
    topics: Vec<GroupOffsetCommitTopicName>,
) -> (
    GroupOffsetCommitEffect,
    OperationDeadline,
    ClassicGroupCommitSession,
    Vec<GroupOffsetCommitTopicName>,
) {
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(100));
    let checkpoint = GroupCheckpoint::try_new(group_id(), member_id(), generation(4), entries)
        .unwrap_or_else(|error| panic!("valid checkpoint: {error}"));
    (
        GroupOffsetCommitEffect::Submit {
            operation_id: OperationId::from_raw(9),
            deadline: deadline.core(),
            checkpoint,
        },
        deadline,
        ClassicGroupCommitSession::new(
            group_id(),
            group,
            member_id(),
            member,
            generation(4),
            classic_generation,
        ),
        topics,
    )
}

pub(super) fn session(
    classic_generation: i64,
    group: Arc<str>,
    member: Arc<str>,
) -> ClassicGroupCommitSession {
    ClassicGroupCommitSession::new(
        group_id(),
        group,
        member_id(),
        member,
        generation(4),
        classic_generation,
    )
}

pub(super) fn entry(
    topic: u64,
    partition: u32,
    next_offset: i64,
    leader_epoch: Option<i32>,
) -> GroupCheckpointEntry {
    GroupCheckpointEntry::try_new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
        next_offset,
        leader_epoch,
    )
    .unwrap_or_else(|error| panic!("valid checkpoint entry: {error}"))
}

pub(super) fn topic(topic: u64, name: Arc<str>) -> GroupOffsetCommitTopicName {
    GroupOffsetCommitTopicName::new(TopicId::from_raw(topic), name)
}

pub(super) fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group id"))
}

pub(super) fn member_id() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member id"))
}

pub(super) fn generation(value: u64) -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(value).unwrap_or_else(|| panic!("nonzero generation"))
}
