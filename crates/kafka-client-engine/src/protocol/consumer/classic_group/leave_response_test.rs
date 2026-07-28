//! Dynamic and static classic-member `LeaveGroup` response scenarios.

use kafka_wire::{LeaveGroupResponse, leave_group_response::MemberResponse};

use super::leave_response::{
    ClassicLeaveGroupOutcome, ClassicLeaveGroupResponseFailure,
    normalize_classic_leave_group_response,
};

#[test]
fn exact_signed_broker_code_and_nonnegative_throttle_are_preserved() {
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 17;
    response.error_code = -731;
    let outcome = normalize_classic_leave_group_response(2, &response)
        .unwrap_or_else(|error| panic!("normalize: {error:?}"));

    assert!(matches!(
        outcome,
        ClassicLeaveGroupOutcome::Rejected {
            throttle_time_ms: 17,
            error_code,
        } if error_code.get() == -731
    ));
}

#[test]
fn v0_throttle_and_v3_member_shapes_are_not_inferred() {
    let mut throttled = LeaveGroupResponse::default();
    throttled.throttle_time_ms = 1;
    assert_eq!(
        normalize_classic_leave_group_response(0, &throttled),
        Err(ClassicLeaveGroupResponseFailure::UnexpectedThrottleTime(1))
    );

    let mut members = LeaveGroupResponse::default();
    members.members = vec![MemberResponse::default()];
    assert_eq!(
        normalize_classic_leave_group_response(2, &members),
        Err(ClassicLeaveGroupResponseFailure::UnexpectedMembers)
    );
}

#[test]
fn v3_static_member_result_is_the_graceful_leave_terminal() {
    let mut response = LeaveGroupResponse::default();
    let mut member = MemberResponse::default();
    member.member_id = "member-a".into();
    member.group_instance_id = Some("instance-a".into());
    response.members.push(member);
    assert!(matches!(
        normalize_classic_leave_group_response(3, &response),
        Ok(ClassicLeaveGroupOutcome::Succeeded { .. })
    ));

    response.members[0].error_code = 82;
    assert!(matches!(
        normalize_classic_leave_group_response(3, &response),
        Ok(ClassicLeaveGroupOutcome::Rejected { error_code, .. })
            if error_code.get() == 82
    ));
}
