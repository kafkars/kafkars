//! Scalar ownership and lossless rejection evidence for resolved assignments.

use crate::{Moment, NextFetchOffset, PartitionIndex, TopicId};

use super::{
    AssignedTopicPartition, AssignmentEpoch, InstallResolvedAssignment,
    InstallResolvedAssignmentError, InstallResolvedAssignmentErrorKind, ResolvedAssignedPartition,
};

#[test]
fn scalar_fact_and_complete_input_recover_every_exact_value() {
    let first = resolved(1, 2, 11);
    let mut partitions = Vec::with_capacity(7);
    partitions.push(first);
    let capacity = partitions.capacity();
    let input = InstallResolvedAssignment::new(
        Some(AssignmentEpoch::initial()),
        partitions,
        Moment::from_tick(13),
        17,
    );

    assert_eq!(
        input.expected_assignment_epoch(),
        Some(AssignmentEpoch::initial())
    );
    assert_eq!(input.partitions(), &[first]);
    assert_eq!(input.partitions_capacity(), capacity);
    assert_eq!(input.now(), Moment::from_tick(13));
    assert_eq!(input.throttle_ticks(), 17);

    let (expected_epoch, partitions, now, throttle_ticks) = input.into_parts();
    assert_eq!(expected_epoch, Some(AssignmentEpoch::initial()));
    assert_eq!(partitions.capacity(), capacity);
    assert_eq!(partitions, [first]);
    assert_eq!(now, Moment::from_tick(13));
    assert_eq!(throttle_ticks, 17);
}

#[test]
fn allocation_rejection_wrapper_preserves_vector_capacity_and_values() {
    let first = resolved(2, 3, 19);
    let mut partitions = Vec::with_capacity(11);
    partitions.push(first);
    let capacity = partitions.capacity();
    let input = InstallResolvedAssignment::new(None, partitions, Moment::from_tick(23), 29);
    let error = InstallResolvedAssignmentError::new(
        InstallResolvedAssignmentErrorKind::AssignmentAllocationFailed,
        input,
    );

    assert_eq!(
        error.kind(),
        InstallResolvedAssignmentErrorKind::AssignmentAllocationFailed
    );
    assert_eq!(error.input().partitions(), &[first]);
    assert_eq!(error.input().partitions_capacity(), capacity);
    let recovered = error.into_input();
    assert_eq!(recovered.partitions_capacity(), capacity);
    assert_eq!(recovered.partitions(), &[first]);
    assert_eq!(recovered.now(), Moment::from_tick(23));
    assert_eq!(recovered.throttle_ticks(), 29);
}

fn resolved(topic: u64, partition: u32, offset: i64) -> ResolvedAssignedPartition {
    ResolvedAssignedPartition::new(
        AssignedTopicPartition::new(
            TopicId::from_raw(topic),
            PartitionIndex::from_raw(partition),
        ),
        NextFetchOffset::try_from_raw(offset)
            .unwrap_or_else(|| panic!("test offset must be nonnegative")),
    )
}
