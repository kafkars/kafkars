//! Live group-assignment identity, ordering, and uniqueness evidence.

use crate::{
    AssignmentGeneration, GroupAssignmentPartition, GroupId, LiveGroupAssignment,
    LiveGroupAssignmentError, MemberId, PartitionIndex, TopicId,
};

#[test]
fn assignment_owns_exact_identities_and_ordered_unique_partitions() {
    let assignment = LiveGroupAssignment::try_new(
        group(3),
        member(5),
        generation(7),
        vec![assigned(1, 0), assigned(1, 2), assigned(2, 0)],
    )
    .unwrap_or_else(|error| panic!("valid assignment: {error}"));

    assert_eq!(assignment.group_id(), group(3));
    assert_eq!(assignment.member_id(), member(5));
    assert_eq!(assignment.assignment_generation(), generation(7));
    assert_eq!(
        assignment.partitions(),
        [assigned(1, 0), assigned(1, 2), assigned(2, 0)]
    );
}

#[test]
fn assignment_rejects_duplicate_and_out_of_order_partitions() {
    assert_eq!(
        LiveGroupAssignment::try_new(
            group(1),
            member(1),
            generation(1),
            vec![assigned(1, 0), assigned(1, 0)],
        ),
        Err(LiveGroupAssignmentError::DuplicatePartition {
            topic_id: topic(1),
            partition: partition(0),
        })
    );
    assert!(matches!(
        LiveGroupAssignment::try_new(
            group(1),
            member(1),
            generation(1),
            vec![assigned(2, 0), assigned(1, 1)],
        ),
        Err(LiveGroupAssignmentError::OutOfOrder { .. })
    ));
}

#[test]
fn assignment_exposes_actual_retained_partition_capacity_without_shrinking() {
    let mut partitions = Vec::with_capacity(9);
    partitions.push(assigned(1, 0));
    let actual_capacity = partitions.capacity();
    let assignment = LiveGroupAssignment::try_new(group(1), member(1), generation(1), partitions)
        .unwrap_or_else(|error| panic!("valid assignment: {error}"));

    assert_eq!(assignment.partitions_capacity(), actual_capacity);
}

fn assigned(topic_id: u64, partition_index: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(topic(topic_id), partition(partition_index))
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
