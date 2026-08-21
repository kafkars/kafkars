//! Public current classic-group state fence contract.

use super::{ConsumerAssignment, ConsumerAssignmentPartition, GroupMembershipEpoch, GroupMetadata};

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
fn group_metadata_retains_transaction_fencing_facts_without_static_identity() {
    let metadata = GroupMetadata::from_parts(
        "workers".into(),
        "member-1".into(),
        GroupMembershipEpoch::Classic { generation_id: 7 },
        3,
        None,
    );
    assert_eq!(metadata.group_id(), "workers");
    assert_eq!(metadata.member_id(), "member-1");
    assert_eq!(
        metadata.membership_epoch(),
        GroupMembershipEpoch::Classic { generation_id: 7 }
    );
    assert_eq!(metadata.membership_epoch().classic_generation_id(), Some(7));
    assert_eq!(metadata.membership_epoch().consumer_member_epoch(), None);
    assert_eq!(metadata.assignment_epoch(), 3);
    assert_eq!(metadata.group_instance_id(), None);
}

#[test]
fn group_metadata_retains_the_configured_static_identity() {
    let metadata = GroupMetadata::from_parts(
        "workers".into(),
        "member-1".into(),
        GroupMembershipEpoch::Classic { generation_id: 7 },
        3,
        Some("instance-a".into()),
    );
    assert_eq!(metadata.group_instance_id(), Some("instance-a"));
}

#[test]
fn consumer_membership_epoch_cannot_be_observed_as_a_classic_generation() {
    let epoch = GroupMembershipEpoch::Consumer { member_epoch: 11 };

    assert_eq!(epoch.classic_generation_id(), None);
    assert_eq!(epoch.consumer_member_epoch(), Some(11));
    let _: fn(&GroupMetadata) -> GroupMembershipEpoch = GroupMetadata::membership_epoch;
}
