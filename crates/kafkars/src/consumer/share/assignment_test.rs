//! Public share assignment vocabulary and fencing evidence.

use super::{ShareConsumerAssignment, ShareConsumerAssignmentPartition};

#[test]
fn assignment_retains_member_and_local_fences_with_ordered_partitions() {
    let assignment = ShareConsumerAssignment::from_parts(
        7,
        3,
        vec![ShareConsumerAssignmentPartition::from_parts(
            "orders".to_owned(),
            2,
        )],
    );

    assert_eq!(assignment.member_epoch(), 7);
    assert_eq!(assignment.assignment_epoch(), 3);
    assert_eq!(assignment.partitions()[0].topic(), "orders");
    assert_eq!(assignment.partitions()[0].partition(), 2);
}
