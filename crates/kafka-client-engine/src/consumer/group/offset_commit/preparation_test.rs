//! Maximum accepted snapshot charging before deterministic admission.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitMachine, LiveGroupAssignment, MemberId,
    OperationId, PartitionIndex, TopicId,
};

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{
        ClassicGroupCommitSession, GroupOffsetCommitEntryReservation,
        GroupOffsetCommitResultReservation, GroupOffsetCommitTopicName, PreparedGroupOffsetCommit,
        PreparedGroupOffsetCommitRequest,
    },
};

use super::host::{GROUP_OFFSET_COMMIT_OPERATION_BYTES, GroupOffsetCommitHost};

#[test]
fn maximum_catalog_snapshot_fits_the_pre_core_operation_reservation() {
    const ENTRY_COUNT: usize = 64;
    const TOPIC_BYTES: usize = 249;
    let group: Arc<str> = Arc::from("g".repeat(i16::MAX as usize));
    let member: Arc<str> = Arc::from("m".repeat(i16::MAX as usize));
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"));
    let member_id = MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member id"));
    let generation = AssignmentGeneration::try_from_raw(1).unwrap_or_else(|| panic!("generation"));
    let mut entries = Vec::new();
    let mut partitions = Vec::new();
    let mut topic_names = Vec::new();
    for index in 0..ENTRY_COUNT {
        let topic_id = TopicId::from_raw((index + 1) as u64);
        let partition = PartitionIndex::from_raw(0);
        let prefix = format!("t{index:02}");
        let name: Arc<str> = Arc::from(format!(
            "{prefix}{}",
            "x".repeat(TOPIC_BYTES - prefix.len())
        ));
        let next_offset =
            i64::try_from(index).unwrap_or_else(|error| panic!("bounded offset: {error}"));
        entries.push(
            GroupCheckpointEntry::try_new(topic_id, partition, next_offset, Some(0))
                .unwrap_or_else(|error| panic!("checkpoint entry: {error}")),
        );
        partitions.push(GroupAssignmentPartition::new(topic_id, partition));
        topic_names.push(GroupOffsetCommitTopicName::new(topic_id, name));
    }
    let checkpoint = GroupCheckpoint::try_new(group_id, member_id, generation, entries)
        .unwrap_or_else(|error| panic!("checkpoint: {error}"));
    let assignment = LiveGroupAssignment::try_new(group_id, member_id, generation, partitions)
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    let session = ClassicGroupCommitSession::new(group_id, group, member_id, member, generation, 1);
    let request = PreparedGroupOffsetCommitRequest::try_new(&session, &checkpoint, &topic_names)
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(100));
    let admission = GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(1),
        deadline.core(),
        Some(&assignment),
        checkpoint,
    )
    .unwrap_or_else(|error| panic!("core admission: {error}"));
    let (machine, effect) = admission.into_parts();
    let prepared = PreparedGroupOffsetCommit::from_effect(
        effect,
        deadline,
        session,
        topic_names,
        GroupOffsetCommitEntryReservation::try_new(ENTRY_COUNT)
            .unwrap_or_else(|error| panic!("entry reservation: {error:?}")),
        GroupOffsetCommitResultReservation::try_new(ENTRY_COUNT)
            .unwrap_or_else(|error| panic!("result reservation: {error:?}")),
    )
    .unwrap_or_else(|error| panic!("prepared: {:?}", error.kind()));
    let actual = GroupOffsetCommitHost::actual_operation_bytes(&machine, &prepared, &request)
        .unwrap_or_else(|| panic!("bounded byte charge"));

    assert!(
        actual <= GROUP_OFFSET_COMMIT_OPERATION_BYTES,
        "{actual} exceeds {GROUP_OFFSET_COMMIT_OPERATION_BYTES}"
    );
}
