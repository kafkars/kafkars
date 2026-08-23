//! Response normalization evidence for `ShareGroupHeartbeat` v1.

use kafka_wire::{
    ShareGroupHeartbeatResponse,
    share_group_heartbeat_response::{Assignment, TopicPartitions},
};
use kafka_wire_core::Uuid;

use super::{
    ShareGroupHeartbeatOutcome, ShareGroupHeartbeatResponseFailure, ShareGroupHeartbeatSuccess,
    normalize_share_group_heartbeat_response,
};

#[test]
fn success_retains_member_epoch_cadence_and_sorted_assignment() {
    let mut response = ShareGroupHeartbeatResponse::default();
    response.member_id = Some("member-1".into());
    response.member_epoch = 3;
    response.heartbeat_interval_ms = 5_000;
    let mut topic = TopicPartitions::default();
    topic.topic_id = Uuid::from_bytes([7; 16]);
    topic.partitions = vec![2, 0];
    let mut assignment = Assignment::default();
    assignment.topic_partitions = vec![topic];
    response.assignment = Some(assignment);
    let outcome = normalize_share_group_heartbeat_response(1, &response)
        .unwrap_or_else(|error| panic!("normalization failed: {error:?}"));
    let ShareGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("expected success")
    };
    assert_eq!(success.member_id().map(AsRef::as_ref), Some("member-1"));
    assert_eq!(success.member_epoch(), 3);
    assert_eq!(success.heartbeat_interval_ms(), 5_000);
    assert_eq!(
        success.assignment().unwrap_or_default()[0].partitions(),
        &[0, 2]
    );
}

#[test]
fn broker_rejection_preserves_exact_signed_code_without_parsing_payload() {
    let mut response = ShareGroupHeartbeatResponse::default();
    response.throttle_time_ms = 9;
    response.error_code = 16;
    response.member_epoch = -99;
    response.heartbeat_interval_ms = -1;
    let outcome = normalize_share_group_heartbeat_response(1, &response)
        .unwrap_or_else(|error| panic!("rejection normalization failed: {error:?}"));
    let ShareGroupHeartbeatOutcome::Rejected(rejection) = outcome else {
        panic!("expected rejection")
    };
    assert_eq!(rejection.error_code().get(), 16);
    assert_eq!(rejection.throttle_time_ms(), 9);
}

#[test]
fn malformed_success_and_wrong_version_fail_closed() {
    let mut response = ShareGroupHeartbeatResponse::default();
    response.member_id = Some("".into());
    assert_eq!(
        normalize_share_group_heartbeat_response(1, &response),
        Err(ShareGroupHeartbeatResponseFailure::InvalidMemberId)
    );
    assert_eq!(
        normalize_share_group_heartbeat_response(0, &ShareGroupHeartbeatResponse::default()),
        Err(ShareGroupHeartbeatResponseFailure::UnsupportedApiVersion(0))
    );
}

pub(crate) fn share_group_heartbeat_success_for_test(
    member: Option<&str>,
    member_epoch: i32,
    heartbeat_interval_ms: i32,
    topics: Vec<([u8; 16], Vec<i32>)>,
) -> ShareGroupHeartbeatSuccess {
    let mut assignment = Assignment::default();
    assignment.topic_partitions = topics
        .into_iter()
        .map(|(topic_id, partitions)| {
            let mut topic = TopicPartitions::default();
            topic.topic_id = Uuid::from_bytes(topic_id);
            topic.partitions = partitions;
            topic
        })
        .collect();
    let mut response = ShareGroupHeartbeatResponse::default();
    response.member_id = member.map(Into::into);
    response.member_epoch = member_epoch;
    response.heartbeat_interval_ms = heartbeat_interval_ms;
    response.assignment = Some(assignment);
    let outcome = normalize_share_group_heartbeat_response(1, &response)
        .unwrap_or_else(|error| panic!("test response normalization failed: {error:?}"));
    let ShareGroupHeartbeatOutcome::Succeeded(success) = outcome else {
        panic!("test response must succeed")
    };
    success
}
