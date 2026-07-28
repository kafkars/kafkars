//! Bounded `LeaveGroup` v3-v5 validation and static-member correlation.

use core::num::NonZeroI16;

use kafka_client_core::{
    ConsumerGroupMemberRemovalBrokerError, ConsumerGroupMemberRemovalOutcome,
    RemoveConsumerGroupMembersBatch, RemoveConsumerGroupMembersPlan,
};
use kafka_wire::LeaveGroupResponse;

use super::ValidatedRemoveConsumerGroupMembersResponse;

/// Generated response facts unsafe to bind to the requested member set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveConsumerGroupMembersProtocolFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    MemberCount { expected: usize, actual: usize },
    UnexpectedMemberId,
    MissingGroupInstanceId,
    UnexpectedGroupInstanceId,
    DuplicateGroupInstanceId,
    RetainedBytes,
}

/// Validates and restores one response to exact caller member order.
pub(crate) fn validate_remove_consumer_group_members_response(
    plan: &RemoveConsumerGroupMembersPlan,
    response: &LeaveGroupResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedRemoveConsumerGroupMembersResponse, RemoveConsumerGroupMembersProtocolFailure>
{
    if !(3..=5).contains(&selected_version) {
        return Err(
            RemoveConsumerGroupMembersProtocolFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        RemoveConsumerGroupMembersProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    if let Some(code) = NonZeroI16::new(response.error_code) {
        ensure_limit(core::mem::size_of::<NonZeroI16>(), result_limit)?;
        return Ok(ValidatedRemoveConsumerGroupMembersResponse::BrokerRejected(
            code,
        ));
    }
    if response.members.len() != plan.members().len() {
        return Err(RemoveConsumerGroupMembersProtocolFailure::MemberCount {
            expected: plan.members().len(),
            actual: response.members.len(),
        });
    }
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(response.members.len())
        .map_err(|_| RemoveConsumerGroupMembersProtocolFailure::RetainedBytes)?;
    for member in &response.members {
        if !member.member_id.is_empty() {
            return Err(RemoveConsumerGroupMembersProtocolFailure::UnexpectedMemberId);
        }
        let group_instance_id = member
            .group_instance_id
            .as_ref()
            .ok_or(RemoveConsumerGroupMembersProtocolFailure::MissingGroupInstanceId)?;
        returned.push((group_instance_id.as_str(), member.error_code));
    }
    returned.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    if returned.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(RemoveConsumerGroupMembersProtocolFailure::DuplicateGroupInstanceId);
    }
    let retained = result_charge(plan)?;
    ensure_limit(retained, result_limit)?;
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(plan.members().len())
        .map_err(|_| RemoveConsumerGroupMembersProtocolFailure::RetainedBytes)?;
    for expected in plan.members() {
        let index = returned
            .binary_search_by(|entry| {
                entry
                    .0
                    .as_bytes()
                    .cmp(expected.group_instance_id().as_bytes())
            })
            .map_err(|_| RemoveConsumerGroupMembersProtocolFailure::UnexpectedGroupInstanceId)?;
        let (group_instance_id, error_code) = returned[index];
        outcomes.push(match NonZeroI16::new(error_code) {
            None => ConsumerGroupMemberRemovalOutcome::removed(group_instance_id.to_owned()),
            Some(code) => ConsumerGroupMemberRemovalOutcome::failed(
                group_instance_id.to_owned(),
                ConsumerGroupMemberRemovalBrokerError::new(code),
            ),
        });
    }
    Ok(ValidatedRemoveConsumerGroupMembersResponse::Batch(
        RemoveConsumerGroupMembersBatch::new(throttle_time_ms, outcomes),
    ))
}

fn result_charge(
    plan: &RemoveConsumerGroupMembersPlan,
) -> Result<usize, RemoveConsumerGroupMembersProtocolFailure> {
    plan.members()
        .iter()
        .try_fold(
            core::mem::size_of::<RemoveConsumerGroupMembersBatch>(),
            |bytes, member| {
                bytes
                    .checked_add(core::mem::size_of::<ConsumerGroupMemberRemovalOutcome>())
                    .and_then(|bytes| bytes.checked_add(member.group_instance_id().len()))
            },
        )
        .ok_or(RemoveConsumerGroupMembersProtocolFailure::RetainedBytes)
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), RemoveConsumerGroupMembersProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(RemoveConsumerGroupMembersProtocolFailure::RetainedBytes)
}
