//! Public current classic-group state fence contract.

use super::{ConsumerAssignment, ConsumerAssignmentPartition, GroupMetadata};

#[test]
fn assignment_retains_named_epoch_fence() {
    let assignment = ConsumerAssignment::from_parts(
        7,
        vec![ConsumerAssignmentPartition::from_parts(
            "orders".to_owned(),
            3,
        )],
    );
    assert_eq!(assignment.assignment_epoch(), 7);
    assert_eq!(assignment.partitions()[0].topic(), "orders");
    assert_eq!(assignment.partitions()[0].partition(), 3);
}

#[test]
fn group_metadata_retains_dynamic_membership_facts() {
    let metadata = GroupMetadata::from_parts("workers".into(), "member-1".into(), 7, 3);
    assert_eq!(metadata.group_id(), "workers");
    assert_eq!(metadata.member_id(), "member-1");
    assert_eq!(metadata.generation_id(), 7);
    assert_eq!(metadata.assignment_epoch(), 3);
}
