//! Exact-code, caller-order, shape, and retained-capacity response scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConsumerGroupMemberRemoval, ConsumerGroupMemberRemovalResult, RemoveConsumerGroupMembersPlan,
};
use kafka_wire::{LeaveGroupResponse, leave_group_response::MemberResponse};

use super::{
    RemoveConsumerGroupMembersProtocolFailure, ValidatedRemoveConsumerGroupMembersResponse,
    validate_remove_consumer_group_members_response,
};

#[test]
fn response_restores_caller_order_and_exact_signed_member_codes() {
    let response = response(
        17,
        0,
        vec![member("instance-a", -31_000), member("instance-b", 0)],
    );
    let validated =
        validate_remove_consumer_group_members_response(&plan(), &response, 5, usize::MAX)
            .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let ValidatedRemoveConsumerGroupMembersResponse::Batch(batch) = validated else {
        panic!("expected member batch");
    };
    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(batch.outcomes()[0].group_instance_id(), "instance-b");
    assert_eq!(batch.outcomes()[1].group_instance_id(), "instance-a");
    let ConsumerGroupMemberRemovalResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("second result should retain broker error");
    };
    assert_eq!(error.code(), -31_000);
}

#[test]
fn top_level_error_is_exact_and_member_shape_is_not_interpreted() {
    let mut hostile_member = MemberResponse::default();
    hostile_member.member_id = "unexpected".into();
    let response = response(0, -719, vec![hostile_member]);
    let validated =
        validate_remove_consumer_group_members_response(&plan(), &response, 3, usize::MAX)
            .unwrap_or_else(|error| panic!("top-level rejection: {error:?}"));
    assert_eq!(
        validated,
        ValidatedRemoveConsumerGroupMembersResponse::BrokerRejected(
            NonZeroI16::new(-719).unwrap_or_else(|| panic!("nonzero"))
        )
    );
}

#[test]
fn malformed_identity_version_throttle_and_capacity_are_rejected() {
    let valid = response(0, 0, vec![member("instance-b", 0), member("instance-a", 0)]);
    assert!(matches!(
        validate_remove_consumer_group_members_response(&plan(), &valid, 2, usize::MAX),
        Err(RemoveConsumerGroupMembersProtocolFailure::UnsupportedApiVersion { actual: 2 })
    ));
    let negative_throttle = response(
        -1,
        0,
        vec![member("instance-b", 0), member("instance-a", 0)],
    );
    assert!(matches!(
        validate_remove_consumer_group_members_response(&plan(), &negative_throttle, 3, usize::MAX),
        Err(RemoveConsumerGroupMembersProtocolFailure::NegativeThrottleTime { actual: -1 })
    ));
    let duplicate = response(0, 0, vec![member("instance-b", 0), member("instance-b", 0)]);
    assert_eq!(
        validate_remove_consumer_group_members_response(&plan(), &duplicate, 3, usize::MAX).err(),
        Some(RemoveConsumerGroupMembersProtocolFailure::DuplicateGroupInstanceId)
    );
    assert_eq!(
        validate_remove_consumer_group_members_response(&plan(), &valid, 3, 0).err(),
        Some(RemoveConsumerGroupMembersProtocolFailure::RetainedBytes)
    );
}

fn plan() -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        "payments".to_owned(),
        vec![
            ConsumerGroupMemberRemoval::new("instance-b".to_owned()),
            ConsumerGroupMemberRemoval::new("instance-a".to_owned()),
        ],
        None,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn member(group_instance_id: &str, error_code: i16) -> MemberResponse {
    let mut member = MemberResponse::default();
    member.group_instance_id = Some(group_instance_id.into());
    member.error_code = error_code;
    member
}

fn response(
    throttle_time_ms: i32,
    error_code: i16,
    members: Vec<MemberResponse>,
) -> LeaveGroupResponse {
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response.members = members;
    response
}
