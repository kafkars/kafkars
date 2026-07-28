//! Bounded `JoinGroup` response correlation and subscription normalization.

use core::num::NonZeroI16;
use std::sync::Arc;

use kafka_client_core::ClassicGeneration;
use kafka_wire::JoinGroupResponse;
use kafka_wire_core::DecodeError;

use super::{
    ClassicBrokerRejection, ClassicJoinOutcome, ClassicJoinedGroup, ClassicJoinedRole,
    join_response_members::normalize_members,
    validation::{RANGE_PROTOCOL, valid_join_version, valid_kafka_string},
};

const MEMBER_ID_REQUIRED_ERROR_CODE: i16 = 79;

/// Generated success facts that cannot safely enter candidate ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicJoinResponseFailure {
    UnsupportedApiVersion(i16),
    UnexpectedThrottleTime(i32),
    NegativeThrottleTime(i32),
    UnexpectedProtocolType,
    UnexpectedProtocolName,
    SkipAssignment,
    InvalidGeneration(i32),
    InvalidLeader,
    InvalidMember,
    UnexpectedFollowerMembers,
    MemberCount { actual: usize, limit: usize },
    StaticMember,
    DuplicateMember,
    LeaderMemberMissing,
    InvalidMemberSlot,
    Metadata(DecodeError),
    UnsupportedSubscriptionVersion(i16),
    SubscriptionUserData,
    TopicCount { actual: usize, limit: usize },
    InvalidTopic,
    DuplicateTopic,
    MemberNameBytes,
    TopicNameBytes,
    Allocation,
}

/// Normalizes one selected v1-v3 Join terminal without deciding failure policy.
pub(crate) fn normalize_classic_join_response(
    selected_version: i16,
    response: &JoinGroupResponse,
) -> Result<ClassicJoinOutcome, ClassicJoinResponseFailure> {
    if !valid_join_version(selected_version) {
        return Err(ClassicJoinResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = normalize_throttle(selected_version, response.throttle_time_ms)?;
    if response.error_code == MEMBER_ID_REQUIRED_ERROR_CODE
        && selected_version == super::validation::STATIC_JOIN_VERSION
    {
        let member = response.member_id.as_str();
        if !valid_kafka_string(member) {
            return Err(ClassicJoinResponseFailure::InvalidMember);
        }
        return Ok(ClassicJoinOutcome::MemberIdRequired {
            member: Arc::from(member),
        });
    }
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ClassicJoinOutcome::Rejected(ClassicBrokerRejection::new(
            throttle_time_ms,
            error_code,
        )));
    }
    validate_success_protocol(response)?;
    let generation = ClassicGeneration::try_from_raw(response.generation_id).ok_or(
        ClassicJoinResponseFailure::InvalidGeneration(response.generation_id),
    )?;
    let member = response.member_id.as_str();
    let leader = response.leader.as_str();
    if !valid_kafka_string(member) {
        return Err(ClassicJoinResponseFailure::InvalidMember);
    }
    if !valid_kafka_string(leader) {
        return Err(ClassicJoinResponseFailure::InvalidLeader);
    }
    let role = if leader == member {
        ClassicJoinedRole::leader(normalize_members(
            &response.members,
            member,
            selected_version,
        )?)
    } else {
        if !response.members.is_empty() {
            return Err(ClassicJoinResponseFailure::UnexpectedFollowerMembers);
        }
        ClassicJoinedRole::follower()
    };
    Ok(ClassicJoinOutcome::Joined(ClassicJoinedGroup::new(
        throttle_time_ms,
        generation,
        Arc::from(member),
        role,
    )))
}

fn normalize_throttle(
    version: i16,
    throttle_time_ms: i32,
) -> Result<u32, ClassicJoinResponseFailure> {
    if version < 2 && throttle_time_ms != 0 {
        return Err(ClassicJoinResponseFailure::UnexpectedThrottleTime(
            throttle_time_ms,
        ));
    }
    u32::try_from(throttle_time_ms)
        .map_err(|_| ClassicJoinResponseFailure::NegativeThrottleTime(throttle_time_ms))
}

fn validate_success_protocol(
    response: &JoinGroupResponse,
) -> Result<(), ClassicJoinResponseFailure> {
    if response.protocol_type.is_some() {
        return Err(ClassicJoinResponseFailure::UnexpectedProtocolType);
    }
    if response
        .protocol_name
        .as_ref()
        .map(kafka_wire_core::StrBytes::as_str)
        != Some(RANGE_PROTOCOL)
    {
        return Err(ClassicJoinResponseFailure::UnexpectedProtocolName);
    }
    if response.skip_assignment {
        return Err(ClassicJoinResponseFailure::SkipAssignment);
    }
    Ok(())
}
