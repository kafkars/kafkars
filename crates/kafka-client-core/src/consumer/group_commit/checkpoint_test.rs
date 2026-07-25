//! Checkpoint scalar validation, assignment binding, and ordering evidence.

use crate::{
    AssignmentGeneration, GroupCheckpoint, GroupCheckpointEntry, GroupCheckpointEntryError,
    GroupCheckpointError, GroupId, MemberId, PartitionIndex, TopicId,
};

#[test]
fn assignment_identities_are_nonzero_scalars() {
    assert_eq!(GroupId::try_from_raw(0), None);
    assert_eq!(MemberId::try_from_raw(0), None);
    assert_eq!(AssignmentGeneration::try_from_raw(0), None);
    assert_eq!(GroupId::try_from_raw(3).map(GroupId::get), Some(3));
    assert_eq!(MemberId::try_from_raw(5).map(MemberId::get), Some(5));
    assert_eq!(
        AssignmentGeneration::try_from_raw(7).map(AssignmentGeneration::get),
        Some(7)
    );
}

#[test]
fn entry_accepts_nonnegative_next_offset_and_optional_leader_epoch() {
    let without_epoch = entry(1, 0, 0, None);
    let with_epoch = entry(1, 1, 12, Some(0));

    assert_eq!(without_epoch.next_offset(), 0);
    assert_eq!(without_epoch.leader_epoch(), None);
    assert_eq!(with_epoch.next_offset(), 12);
    assert_eq!(with_epoch.leader_epoch(), Some(0));
}

#[test]
fn entry_rejects_negative_offsets_and_epochs() {
    assert_eq!(
        GroupCheckpointEntry::try_new(topic(1), partition(0), -1, None),
        Err(GroupCheckpointEntryError::NegativeNextOffset { value: -1 })
    );
    assert_eq!(
        GroupCheckpointEntry::try_new(topic(1), partition(0), 0, Some(-1)),
        Err(GroupCheckpointEntryError::NegativeLeaderEpoch { value: -1 })
    );
}

#[test]
fn checkpoint_binds_ordered_entries_to_one_assignment() {
    let checkpoint = GroupCheckpoint::try_new(
        group(11),
        member(13),
        generation(17),
        vec![
            entry(1, 0, 4, None),
            entry(1, 1, 9, Some(2)),
            entry(2, 0, 3, None),
        ],
    )
    .unwrap_or_else(|error| panic!("ordered checkpoint: {error}"));

    assert_eq!(checkpoint.group_id().get(), 11);
    assert_eq!(checkpoint.member_id().get(), 13);
    assert_eq!(checkpoint.assignment_generation().get(), 17);
    assert_eq!(checkpoint.entries().len(), 3);
    assert_eq!(checkpoint.entries()[1].topic_id(), topic(1));
    assert_eq!(checkpoint.entries()[1].partition(), partition(1));
}

#[test]
fn checkpoint_rejects_empty_duplicate_and_out_of_order_entries() {
    assert_eq!(
        GroupCheckpoint::try_new(group(1), member(1), generation(1), Vec::new()),
        Err(GroupCheckpointError::Empty)
    );
    assert_eq!(
        GroupCheckpoint::try_new(
            group(1),
            member(1),
            generation(1),
            vec![entry(1, 0, 1, None), entry(1, 0, 2, None)],
        ),
        Err(GroupCheckpointError::DuplicateTopicPartition {
            topic_id: topic(1),
            partition: partition(0),
        })
    );
    assert!(matches!(
        GroupCheckpoint::try_new(
            group(1),
            member(1),
            generation(1),
            vec![entry(2, 0, 1, None), entry(1, 1, 2, None)],
        ),
        Err(GroupCheckpointError::OutOfOrder { .. })
    ));
}

fn entry(
    topic_id: u64,
    partition_index: u32,
    next_offset: i64,
    leader_epoch: Option<i32>,
) -> GroupCheckpointEntry {
    GroupCheckpointEntry::try_new(
        topic(topic_id),
        partition(partition_index),
        next_offset,
        leader_epoch,
    )
    .unwrap_or_else(|error| panic!("valid entry: {error}"))
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
