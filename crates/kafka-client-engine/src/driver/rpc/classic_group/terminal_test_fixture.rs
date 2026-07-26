//! Opaque generated-protocol terminals for cross-domain engine tests.

use kafka_wire::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, JoinGroupResponse, SyncGroupResponse,
    consumer_protocol_assignment::TopicPartition, encode_consumer_protocol_assignment,
    encode_consumer_protocol_subscription, join_group_response::JoinGroupResponseMember,
};
use kafka_wire_core::{ApiVersion, Bytes, BytesMut};

use super::{JoinGroupCallKey, SyncGroupCallKey, TrackedJoinGroupCalls, TrackedSyncGroupCalls};

pub(crate) fn install_follower_join_terminal(
    calls: &mut TrackedJoinGroupCalls,
    key: JoinGroupCallKey,
) {
    calls.install_terminal_for_test(key, Some(3), Ok(join_response("member-b", "member-a")));
}

pub(crate) fn install_leader_join_terminal(
    calls: &mut TrackedJoinGroupCalls,
    key: JoinGroupCallKey,
) {
    let mut response = join_response("member-a", "member-a");
    response.members = vec![join_member("member-a", &["orders"])];
    calls.install_terminal_for_test(key, Some(3), Ok(response));
}

pub(crate) fn install_empty_leader_join_terminal(
    calls: &mut TrackedJoinGroupCalls,
    key: JoinGroupCallKey,
) {
    let mut response = join_response("member-a", "member-a");
    response.members = vec![join_member("member-a", &[])];
    calls.install_terminal_for_test(key, Some(3), Ok(response));
}

pub(crate) fn install_join_broker_rejection_terminal(
    calls: &mut TrackedJoinGroupCalls,
    key: JoinGroupCallKey,
    error_code: i16,
) {
    let mut response = JoinGroupResponse::default();
    response.error_code = error_code;
    calls.install_terminal_for_test(key, Some(3), Ok(response));
}

pub(crate) fn install_sync_assignment_terminal(
    calls: &mut TrackedSyncGroupCalls,
    key: SyncGroupCallKey,
    topic: &str,
    partitions: &[i32],
) {
    let mut assigned_topic = TopicPartition::default();
    assigned_topic.topic = topic.into();
    assigned_topic.partitions = partitions.to_vec();
    let mut assignment = ConsumerProtocolAssignment::default();
    assignment.assigned_partitions = vec![assigned_topic];
    assignment.user_data = None;
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_assignment(&mut encoded, &assignment, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("assignment encode failed: {error}"));
    let mut response = SyncGroupResponse::default();
    response.assignment = encoded.freeze();
    calls.install_terminal_for_test(key, Some(2), Ok(response));
}

pub(crate) fn install_malformed_sync_terminal(
    calls: &mut TrackedSyncGroupCalls,
    key: SyncGroupCallKey,
) {
    let mut response = SyncGroupResponse::default();
    response.assignment = Bytes::from_static(b"not an assignment");
    calls.install_terminal_for_test(key, Some(2), Ok(response));
}

pub(crate) fn install_sync_broker_rejection_terminal(
    calls: &mut TrackedSyncGroupCalls,
    key: SyncGroupCallKey,
    error_code: i16,
) {
    let mut response = SyncGroupResponse::default();
    response.error_code = error_code;
    calls.install_terminal_for_test(key, Some(2), Ok(response));
}

fn join_response(local: &str, leader: &str) -> JoinGroupResponse {
    let mut response = JoinGroupResponse::default();
    response.generation_id = 7;
    response.protocol_name = Some("range".into());
    response.leader = leader.into();
    response.member_id = local.into();
    response
}

fn join_member(name: &str, topics: &[&str]) -> JoinGroupResponseMember {
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics = topics.iter().copied().map(Into::into).collect();
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_subscription(&mut encoded, &subscription, ApiVersion::new(0))
        .unwrap_or_else(|error| panic!("subscription encode failed: {error}"));
    let mut member = JoinGroupResponseMember::default();
    member.member_id = name.into();
    member.metadata = encoded.freeze();
    member
}
