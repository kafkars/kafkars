//! Construction, ordering, and retained-owner boundaries.

use crate::{
    AssignmentGeneration, Deadline, GroupAssignmentPartition, GroupId, MemberId, MembershipCycle,
    PartitionIndex, TopicId,
};

use super::{
    GroupPositionBootstrapBuildErrorKind, GroupPositionBootstrapMachine,
    GroupPositionBootstrapState, GroupPositionFence,
};

#[test]
fn construction_retains_one_deadline_fence_and_two_ordered_owners() {
    let fence = position_fence(2);
    let machine = GroupPositionBootstrapMachine::try_new(
        fence,
        Deadline::from_tick(31),
        vec![assigned(3, 0), assigned(3, 2), assigned(5, 0)],
    )
    .unwrap_or_else(|error| panic!("valid bootstrap: {error}"));

    assert_eq!(machine.fence(), fence);
    assert_eq!(machine.deadline(), Deadline::from_tick(31));
    assert_eq!(
        machine.partitions(),
        &[assigned(3, 0), assigned(3, 2), assigned(5, 0)]
    );
    assert!(machine.expected_capacity() >= 3);
    assert_eq!(machine.state(), GroupPositionBootstrapState::Ready);
}

#[test]
fn duplicate_and_out_of_order_assignments_are_rejected_losslessly() {
    for (partitions, expected) in [
        (
            vec![assigned(3, 0), assigned(3, 0)],
            GroupPositionBootstrapBuildErrorKind::DuplicatePartition(assigned(3, 0)),
        ),
        (
            vec![assigned(5, 0), assigned(3, 2)],
            GroupPositionBootstrapBuildErrorKind::OutOfOrder {
                previous: assigned(5, 0),
                current: assigned(3, 2),
            },
        ),
    ] {
        let original = partitions.clone();
        let Err(error) = GroupPositionBootstrapMachine::try_new(
            position_fence(2),
            Deadline::from_tick(31),
            partitions,
        ) else {
            panic!("invalid order must reject");
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(error.into_partitions(), original);
    }
}

fn position_fence(assignment_generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(13).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(assignment_generation)
            .unwrap_or_else(|| panic!("generation")),
    )
}

fn assigned(topic: u64, partition: u32) -> GroupAssignmentPartition {
    GroupAssignmentPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}
