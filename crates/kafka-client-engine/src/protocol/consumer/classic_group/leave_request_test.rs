//! Dynamic and static classic-member `LeaveGroup` request shape scenarios.

use super::leave_request::{
    ClassicLeaveGroupRequestFailure, classic_leave_group_request,
    classic_leave_group_request_with_instance,
};

#[test]
fn request_uses_the_v0_v2_dynamic_member_shape() {
    let prepared = classic_leave_group_request("workers", "member-7")
        .unwrap_or_else(|error| panic!("request: {error:?}"));
    let request = prepared.request_for_test();

    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.member_id.as_str(), "member-7");
    assert!(request.members.is_empty());
}

#[test]
fn static_request_uses_the_exact_v3_member_identity_shape() {
    let prepared =
        classic_leave_group_request_with_instance("workers", "member-7", Some("instance-a"))
            .unwrap_or_else(|error| panic!("static request: {error:?}"));
    let request = prepared.request_for_test();

    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.member_id.as_str(), "");
    assert_eq!(request.members.len(), 1);
    assert_eq!(request.members[0].member_id.as_str(), "member-7");
    assert_eq!(
        request.members[0]
            .group_instance_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("instance-a")
    );
}

#[test]
fn invalid_identity_is_rejected_before_generated_ownership() {
    assert!(matches!(
        classic_leave_group_request("", "member"),
        Err(ClassicLeaveGroupRequestFailure::GroupName)
    ));
    assert!(matches!(
        classic_leave_group_request("workers", ""),
        Err(ClassicLeaveGroupRequestFailure::MemberId)
    ));
    assert!(matches!(
        classic_leave_group_request_with_instance("workers", "member", Some("")),
        Err(ClassicLeaveGroupRequestFailure::GroupInstanceId)
    ));
}
