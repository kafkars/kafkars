//! Exact pending assignment and classic-generation ownership.

use kafka_client_core::{
    AssignmentGeneration, ClassicGeneration, GroupAssignmentPartition, GroupId,
    LiveGroupAssignment, MemberId, PartitionIndex, TopicId,
};

use super::model::PendingClassicGroupRevocation;

#[test]
fn pending_owner_retains_exact_assignment_and_generation() {
    let generation =
        ClassicGeneration::try_from_raw(7).unwrap_or_else(|| panic!("classic generation"));
    let assignment = LiveGroupAssignment::try_new(
        GroupId::try_from_raw(3).unwrap_or_else(|| panic!("group")),
        MemberId::try_from_raw(5).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(11).unwrap_or_else(|| panic!("assignment generation")),
        vec![GroupAssignmentPartition::new(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
        )],
    )
    .unwrap_or_else(|error| panic!("assignment: {error:?}"));

    let pending = PendingClassicGroupRevocation::new(assignment, generation);

    assert_eq!(pending.generation, generation);
    assert_eq!(pending.assignment.assignment_generation().get(), 11);
    assert_eq!(pending.assignment.partitions().len(), 1);
}
