//! Shared generated `JoinGroup` response fixtures for sibling scenario modules.

use kafka_wire::{
    ConsumerProtocolSubscription, JoinGroupResponse, encode_consumer_protocol_subscription,
    join_group_response::JoinGroupResponseMember,
};
use kafka_wire_core::{ApiVersion, BytesMut};

use super::validation::RANGE_PROTOCOL;

pub(super) fn metadata(version: i16, topics: &[&str]) -> kafka_wire_core::Bytes {
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics = topics.iter().copied().map(Into::into).collect();
    let mut encoded = BytesMut::new();
    encode_consumer_protocol_subscription(&mut encoded, &subscription, ApiVersion::new(version))
        .unwrap_or_else(|error| panic!("subscription encode failed: {error}"));
    encoded.freeze()
}

pub(super) fn member(name: &str, topics: &[&str]) -> JoinGroupResponseMember {
    let mut member = JoinGroupResponseMember::default();
    member.member_id = name.into();
    member.metadata = metadata(0, topics);
    member
}

pub(super) fn response(local: &str, leader: &str) -> JoinGroupResponse {
    let mut response = JoinGroupResponse::default();
    response.generation_id = 7;
    response.protocol_name = Some(RANGE_PROTOCOL.into());
    response.leader = leader.into();
    response.member_id = local.into();
    response
}
