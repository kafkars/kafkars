//! Borrowed group-checkpoint preflight and admission-consistency evidence.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupCheckpoint,
    GroupCheckpointEntry, GroupId, GroupOffsetCommitAdmissionErrorKind, GroupOffsetCommitMachine,
    LiveGroupAssignment, MemberId, OperationId, PartitionIndex, TopicId,
    validate_group_offset_commit_checkpoint,
};

#[test]
fn valid_ordered_checkpoint_passes_borrowed_preflight() {
    let assignment = assignment(1, &[(5, 0), (5, 2), (7, 1)]);
    let checkpoint = checkpoint(1, &[(5, 0, 11), (5, 2, 19), (7, 1, 23)]);

    assert_eq!(
        validate_group_offset_commit_checkpoint(Some(&assignment), &checkpoint),
        Ok(())
    );
    assert_eq!(checkpoint.entries()[2].next_offset(), 23);
}

#[test]
fn foreign_group_and_missing_topic_retain_existing_exact_rejections() {
    let assignment = assignment(1, &[(5, 0)]);
    let foreign_group = checkpoint(9, &[(5, 0, 11)]);
    let missing_topic = checkpoint(1, &[(7, 0, 13)]);

    assert_eq!(
        validate_group_offset_commit_checkpoint(Some(&assignment), &foreign_group),
        Err(GroupOffsetCommitAdmissionErrorKind::GroupMismatch)
    );
    assert_eq!(
        validate_group_offset_commit_checkpoint(Some(&assignment), &missing_topic),
        Err(GroupOffsetCommitAdmissionErrorKind::UnassignedPartition {
            topic_id: topic(7),
            partition: partition(0),
        })
    );
    assert_eq!(foreign_group.entries()[0].next_offset(), 11);
    assert_eq!(missing_topic.entries()[0].next_offset(), 13);
}

#[test]
fn preflight_and_authoritative_admission_share_validation_policy() {
    let assignment = assignment(1, &[(5, 0)]);
    let checkpoint = checkpoint(9, &[(5, 0, 17)]);
    let expected = match validate_group_offset_commit_checkpoint(Some(&assignment), &checkpoint) {
        Ok(()) => panic!("foreign checkpoint must fail borrowed preflight"),
        Err(kind) => kind,
    };
    let admission = GroupOffsetCommitMachine::try_admit(
        OperationId::from_raw(3),
        Deadline::from_tick(5),
        Some(&assignment),
        checkpoint,
    );
    let error = match admission {
        Ok(_) => panic!("foreign checkpoint must fail authoritative admission"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), expected);
    assert_eq!(error.into_checkpoint().entries()[0].next_offset(), 17);
}

fn assignment(group_id: u64, partitions: &[(u64, u32)]) -> LiveGroupAssignment {
    LiveGroupAssignment::try_new(
        group(group_id),
        member(),
        generation(),
        partitions
            .iter()
            .map(|&(topic_id, partition_index)| {
                GroupAssignmentPartition::new(topic(topic_id), partition(partition_index))
            })
            .collect(),
    )
    .unwrap_or_else(|error| panic!("valid assignment: {error}"))
}

fn checkpoint(group_id: u64, entries: &[(u64, u32, i64)]) -> GroupCheckpoint {
    GroupCheckpoint::try_new(
        group(group_id),
        member(),
        generation(),
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

fn member() -> MemberId {
    MemberId::try_from_raw(2).unwrap_or_else(|| panic!("nonzero member"))
}

fn generation() -> AssignmentGeneration {
    AssignmentGeneration::try_from_raw(3).unwrap_or_else(|| panic!("nonzero generation"))
}

const fn topic(value: u64) -> TopicId {
    TopicId::from_raw(value)
}

const fn partition(value: u32) -> PartitionIndex {
    PartitionIndex::from_raw(value)
}
