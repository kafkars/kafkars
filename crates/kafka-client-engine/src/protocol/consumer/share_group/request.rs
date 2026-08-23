//! Strict `ShareGroupHeartbeat` v1 join, steady, and leave materialization.

use kafka_wire::ShareGroupHeartbeatRequest;

use super::model::{
    MAX_KAFKA_STRING_BYTES, SHARE_GROUP_HEARTBEAT_MAX_TOPIC_BYTES, SHARE_GROUP_HEARTBEAT_MAX_TOPICS,
};

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareGroupHeartbeatRequestFailure {
    GroupId,
    MemberId,
    MemberEpoch(i32),
    RackId,
    EmptySubscription,
    TooManyTopics { actual: usize, limit: usize },
    TopicName,
    DuplicateTopicName,
    Allocation,
}

/// Linear ownership of one validated generated API 76 v1 request.
#[must_use = "a prepared ShareGroupHeartbeat request must be submitted or released"]
pub(crate) struct PreparedShareGroupHeartbeatRequest {
    request: ShareGroupHeartbeatRequest,
}

impl PreparedShareGroupHeartbeatRequest {
    pub(crate) fn into_generated_request(self) -> ShareGroupHeartbeatRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &ShareGroupHeartbeatRequest {
        &self.request
    }
}

pub(crate) fn share_group_join_request(
    group_id: &str,
    member_id: &str,
    rack_id: Option<&str>,
    topics: &[&str],
) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
    validate_group_and_member(group_id, member_id)?;
    if rack_id.is_some_and(|rack| !valid_kafka_string(rack)) {
        return Err(ShareGroupHeartbeatRequestFailure::RackId);
    }
    let mut request = base_request(group_id, member_id, 0);
    request.rack_id = rack_id.map(Into::into);
    request.subscribed_topic_names = Some(subscription(topics)?);
    Ok(PreparedShareGroupHeartbeatRequest { request })
}

pub(crate) fn share_group_steady_request(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
    validate_group_and_member(group_id, member_id)?;
    if member_epoch <= 0 {
        return Err(ShareGroupHeartbeatRequestFailure::MemberEpoch(member_epoch));
    }
    Ok(PreparedShareGroupHeartbeatRequest {
        request: base_request(group_id, member_id, member_epoch),
    })
}

pub(crate) fn share_group_leave_request(
    group_id: &str,
    member_id: &str,
) -> Result<PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure> {
    validate_group_and_member(group_id, member_id)?;
    Ok(PreparedShareGroupHeartbeatRequest {
        request: base_request(group_id, member_id, -1),
    })
}

fn base_request(group_id: &str, member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatRequest {
    let mut request = ShareGroupHeartbeatRequest::default();
    request.group_id = group_id.into();
    request.member_id = member_id.into();
    request.member_epoch = member_epoch;
    request
}

fn subscription(
    topics: &[&str],
) -> Result<Vec<kafka_wire_core::StrBytes>, ShareGroupHeartbeatRequestFailure> {
    if topics.is_empty() {
        return Err(ShareGroupHeartbeatRequestFailure::EmptySubscription);
    }
    if topics.len() > SHARE_GROUP_HEARTBEAT_MAX_TOPICS {
        return Err(ShareGroupHeartbeatRequestFailure::TooManyTopics {
            actual: topics.len(),
            limit: SHARE_GROUP_HEARTBEAT_MAX_TOPICS,
        });
    }
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(topics.len())
        .map_err(|_allocation| ShareGroupHeartbeatRequestFailure::Allocation)?;
    for (index, topic) in topics.iter().enumerate() {
        if topic.is_empty() || topic.len() > SHARE_GROUP_HEARTBEAT_MAX_TOPIC_BYTES {
            return Err(ShareGroupHeartbeatRequestFailure::TopicName);
        }
        if topics[..index].contains(topic) {
            return Err(ShareGroupHeartbeatRequestFailure::DuplicateTopicName);
        }
        retained.push((*topic).into());
    }
    Ok(retained)
}

fn validate_group_and_member(
    group_id: &str,
    member_id: &str,
) -> Result<(), ShareGroupHeartbeatRequestFailure> {
    if !valid_kafka_string(group_id) {
        return Err(ShareGroupHeartbeatRequestFailure::GroupId);
    }
    if !valid_kafka_string(member_id) {
        return Err(ShareGroupHeartbeatRequestFailure::MemberId);
    }
    Ok(())
}

fn valid_kafka_string(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_KAFKA_STRING_BYTES
}
