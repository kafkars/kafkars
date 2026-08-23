//! Request-shape evidence for the `ShareGroupHeartbeat` v1 adapter.

use super::{
    ShareGroupHeartbeatRequestFailure, share_group_join_request, share_group_leave_request,
    share_group_steady_request,
};

#[test]
fn join_retains_stable_member_subscription_and_optional_rack() {
    let prepared = share_group_join_request("workers", "member-1", Some("rack-a"), &["jobs"])
        .unwrap_or_else(|error| panic!("join request failed: {error:?}"));
    let request = prepared.request_for_test();
    assert_eq!(request.group_id.as_str(), "workers");
    assert_eq!(request.member_id.as_str(), "member-1");
    assert_eq!(request.member_epoch, 0);
    assert_eq!(
        request
            .rack_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("rack-a")
    );
    assert_eq!(
        request
            .subscribed_topic_names
            .as_ref()
            .map(|topics| topics[0].as_str()),
        Some("jobs")
    );
}

#[test]
fn steady_and_leave_send_only_the_retained_identity_and_epoch() {
    let steady = share_group_steady_request("workers", "member-1", 7)
        .unwrap_or_else(|error| panic!("steady request failed: {error:?}"));
    let request = steady.request_for_test();
    assert_eq!(request.member_epoch, 7);
    assert!(request.rack_id.is_none());
    assert!(request.subscribed_topic_names.is_none());

    let leave = share_group_leave_request("workers", "member-1")
        .unwrap_or_else(|error| panic!("leave request failed: {error:?}"));
    assert_eq!(leave.request_for_test().member_epoch, -1);
}

#[test]
fn invalid_or_duplicate_subscription_is_rejected_before_wire_ownership() {
    assert_eq!(
        share_group_join_request("workers", "member-1", None, &[]).err(),
        Some(ShareGroupHeartbeatRequestFailure::EmptySubscription)
    );
    assert_eq!(
        share_group_join_request("workers", "member-1", None, &["jobs", "jobs"]).err(),
        Some(ShareGroupHeartbeatRequestFailure::DuplicateTopicName)
    );
    assert_eq!(
        share_group_steady_request("workers", "member-1", 0).err(),
        Some(ShareGroupHeartbeatRequestFailure::MemberEpoch(0))
    );
}
