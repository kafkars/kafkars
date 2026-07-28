//! Canonical static-member removal request projection tests.

use super::{ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersRequest};

#[test]
fn preserves_group_member_order_and_reason() {
    let request = RemoveConsumerGroupMembersRequest::new(
        String::from("orders"),
        vec![
            ConsumerGroupMemberRemoval::new("instance-b"),
            ConsumerGroupMemberRemoval::new("instance-a"),
        ],
        Some(String::from("maintenance")),
    );

    let Ok(plan) = request.canonicalize().into_plan() else {
        panic!("valid plan expected");
    };

    assert_eq!(plan.group_id(), "orders");
    assert_eq!(plan.members()[0].group_instance_id(), "instance-b");
    assert_eq!(plan.members()[1].group_instance_id(), "instance-a");
    assert_eq!(plan.reason(), Some("maintenance"));
}

#[test]
fn preparation_charge_tracks_member_and_reason_bytes() {
    let base = RemoveConsumerGroupMembersRequest::new(
        String::from("g"),
        vec![ConsumerGroupMemberRemoval::new("a")],
        None,
    );
    let larger = RemoveConsumerGroupMembersRequest::new(
        String::from("group"),
        vec![
            ConsumerGroupMemberRemoval::new("instance-a"),
            ConsumerGroupMemberRemoval::new("instance-b"),
        ],
        Some(String::from("reason")),
    );

    assert!(larger.preparation_charge() > base.preparation_charge());
}
