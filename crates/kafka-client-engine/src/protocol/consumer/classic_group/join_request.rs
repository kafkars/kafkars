//! Generated `JoinGroup` construction for one dynamic Range subscription.

use std::sync::Arc;

use kafka_client_core::ClassicGroupTiming;
use kafka_wire::{
    ConsumerProtocolSubscription, JoinGroupRequest, encode_consumer_protocol_subscription,
    join_group_request::JoinGroupRequestProtocol,
};
use kafka_wire_core::{ApiVersion, BytesMut, EncodeError};

use super::validation::{
    INNER_SCHEMA_VERSION, MAX_TOPICS, PROTOCOL_TYPE, RANGE_PROTOCOL, valid_kafka_string,
    valid_topic,
};

/// Local request-shape failure before driver ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicJoinRequestFailure {
    InvalidGroup,
    InvalidMember,
    InvalidGroupInstance,
    TopicCount { actual: usize, limit: usize },
    InvalidTopic,
    DuplicateTopic,
    OutOfOrderTopic,
    Allocation,
    Encode(EncodeError),
}

/// Linear ownership of one validated generated classic Join request.
#[must_use = "a prepared classic Join request must be submitted or deliberately released"]
pub(crate) struct PreparedClassicJoinGroupRequest {
    request: JoinGroupRequest,
}

impl PreparedClassicJoinGroupRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_join_group_request(self) -> JoinGroupRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &JoinGroupRequest {
        &self.request
    }
}

/// Builds one v1-v3-compatible dynamic Range `JoinGroup` request.
pub(crate) fn classic_join_group_request(
    group: &str,
    member: Option<&str>,
    topics: &[Arc<str>],
    timing: ClassicGroupTiming,
) -> Result<PreparedClassicJoinGroupRequest, ClassicJoinRequestFailure> {
    classic_join_group_request_with_instance(group, member, None, topics, timing)
}

/// Builds one Range `JoinGroup` request with an optional static identity.
pub(crate) fn classic_join_group_request_with_instance(
    group: &str,
    member: Option<&str>,
    group_instance_id: Option<&str>,
    topics: &[Arc<str>],
    timing: ClassicGroupTiming,
) -> Result<PreparedClassicJoinGroupRequest, ClassicJoinRequestFailure> {
    validate_inputs(group, member, group_instance_id, topics)?;
    let mut subscription_topics = Vec::new();
    subscription_topics
        .try_reserve_exact(topics.len())
        .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
    subscription_topics.extend(topics.iter().map(|topic| topic.as_ref().into()));
    let mut subscription = ConsumerProtocolSubscription::default();
    subscription.topics = subscription_topics;
    let mut metadata = BytesMut::new();
    encode_consumer_protocol_subscription(
        &mut metadata,
        &subscription,
        ApiVersion::new(INNER_SCHEMA_VERSION),
    )
    .map_err(ClassicJoinRequestFailure::Encode)?;

    let mut protocols = Vec::new();
    protocols
        .try_reserve_exact(1)
        .map_err(|_error| ClassicJoinRequestFailure::Allocation)?;
    let mut range = JoinGroupRequestProtocol::default();
    range.name = RANGE_PROTOCOL.into();
    range.metadata = metadata.freeze();
    protocols.push(range);

    let mut request = JoinGroupRequest::default();
    request.group_id = group.into();
    request.session_timeout_ms = timing.session_timeout_ms();
    request.rebalance_timeout_ms = timing.rebalance_timeout_ms();
    request.member_id = member.unwrap_or_default().into();
    request.group_instance_id = group_instance_id.map(Into::into);
    request.protocol_type = PROTOCOL_TYPE.into();
    request.protocols = protocols;
    request.reason = None;
    Ok(PreparedClassicJoinGroupRequest { request })
}

fn validate_inputs(
    group: &str,
    member: Option<&str>,
    group_instance_id: Option<&str>,
    topics: &[Arc<str>],
) -> Result<(), ClassicJoinRequestFailure> {
    if !valid_kafka_string(group) {
        return Err(ClassicJoinRequestFailure::InvalidGroup);
    }
    if member.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicJoinRequestFailure::InvalidMember);
    }
    if group_instance_id.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicJoinRequestFailure::InvalidGroupInstance);
    }
    if topics.len() > MAX_TOPICS {
        return Err(ClassicJoinRequestFailure::TopicCount {
            actual: topics.len(),
            limit: MAX_TOPICS,
        });
    }
    for (index, topic) in topics.iter().enumerate() {
        if !valid_topic(topic) {
            return Err(ClassicJoinRequestFailure::InvalidTopic);
        }
        if index > 0 && topics[index - 1].as_ref() == topic.as_ref() {
            return Err(ClassicJoinRequestFailure::DuplicateTopic);
        }
        if index > 0 && topics[index - 1].as_ref() > topic.as_ref() {
            return Err(ClassicJoinRequestFailure::OutOfOrderTopic);
        }
    }
    Ok(())
}
