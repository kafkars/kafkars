//! Scenarios for consumer-group member-removal intent validation.

use super::{
    ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersPlan, RemoveConsumerGroupMembersPlanError,
};

#[test]
fn plan_preserves_group_member_order_and_optional_reason() {
    let plan = RemoveConsumerGroupMembersPlan::new(
        "payments".to_owned(),
        vec![member("instance-b"), member("instance-a")],
        Some("maintenance".to_owned()),
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.members()[0].group_instance_id(), "instance-b");
    assert_eq!(plan.members()[1].group_instance_id(), "instance-a");
    assert_eq!(plan.reason(), Some("maintenance"));
}

#[test]
fn plan_rejects_invalid_or_duplicate_identities() {
    for (group, members, expected) in [
        (
            "",
            vec![member("instance-a")],
            RemoveConsumerGroupMembersPlanError::EmptyGroupId,
        ),
        (
            "payments",
            Vec::new(),
            RemoveConsumerGroupMembersPlanError::EmptyMemberBatch,
        ),
        (
            "payments",
            vec![member("")],
            RemoveConsumerGroupMembersPlanError::EmptyGroupInstanceId,
        ),
        (
            "payments",
            vec![member("instance-a"), member("instance-a")],
            RemoveConsumerGroupMembersPlanError::DuplicateGroupInstanceId,
        ),
    ] {
        assert_eq!(
            RemoveConsumerGroupMembersPlan::new(group.to_owned(), members, None),
            Err(expected)
        );
    }
}

#[test]
fn plan_rejects_strings_outside_kafka_domain() {
    let oversized = "x".repeat(i16::MAX as usize + 1);
    assert_eq!(
        RemoveConsumerGroupMembersPlan::new(oversized.clone(), vec![member("instance-a")], None,),
        Err(RemoveConsumerGroupMembersPlanError::GroupIdTooLong)
    );
    assert_eq!(
        RemoveConsumerGroupMembersPlan::new("payments".to_owned(), vec![member(&oversized)], None,),
        Err(RemoveConsumerGroupMembersPlanError::GroupInstanceIdTooLong)
    );
    assert_eq!(
        RemoveConsumerGroupMembersPlan::new(
            "payments".to_owned(),
            vec![member("instance-a")],
            Some(oversized),
        ),
        Err(RemoveConsumerGroupMembersPlanError::ReasonTooLong)
    );
}

fn member(group_instance_id: &str) -> ConsumerGroupMemberRemoval {
    ConsumerGroupMemberRemoval::new(group_instance_id.to_owned())
}
