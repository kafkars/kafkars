//! Stable public static-member identity tests.

use super::ConsumerGroupMemberRemoval;

#[test]
fn retains_exact_group_instance_identity() {
    let member = ConsumerGroupMemberRemoval::new("instance-a");

    assert_eq!(member.group_instance_id(), "instance-a");
}
