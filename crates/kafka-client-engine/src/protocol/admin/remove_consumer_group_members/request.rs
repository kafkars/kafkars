//! Generated `LeaveGroup` v3-v5 construction from validated static members.

use core::fmt;

use kafka_client_core::RemoveConsumerGroupMembersPlan;
use kafka_wire::{LeaveGroupRequest, leave_group_request::MemberIdentity};

/// Request construction failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoveConsumerGroupMembersRequestFailure {
    /// The generated request projection exceeds the admitted envelope.
    RetainedBytes,
}

impl fmt::Display for RemoveConsumerGroupMembersRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("generated member-removal request exceeds its admitted envelope")
    }
}

impl std::error::Error for RemoveConsumerGroupMembersRequestFailure {}

/// Builds one generated request and returns its required API-version floor.
pub(crate) fn remove_consumer_group_members_request(
    plan: &RemoveConsumerGroupMembersPlan,
    retained_limit: usize,
) -> Result<(LeaveGroupRequest, i16), RemoveConsumerGroupMembersRequestFailure> {
    let required = remove_consumer_group_members_request_charge(plan)
        .ok_or(RemoveConsumerGroupMembersRequestFailure::RetainedBytes)?;
    if required > retained_limit {
        return Err(RemoveConsumerGroupMembersRequestFailure::RetainedBytes);
    }
    let mut request = LeaveGroupRequest::default();
    request.group_id = plan.group_id().into();
    request
        .members
        .try_reserve_exact(plan.members().len())
        .map_err(|_| RemoveConsumerGroupMembersRequestFailure::RetainedBytes)?;
    request.members.extend(plan.members().iter().map(|member| {
        let mut identity = MemberIdentity::default();
        identity.group_instance_id = Some(member.group_instance_id().into());
        identity.reason = plan.reason().map(Into::into);
        identity
    }));
    Ok((request, if plan.reason().is_some() { 5 } else { 3 }))
}

pub(crate) fn remove_consumer_group_members_request_charge(
    plan: &RemoveConsumerGroupMembersPlan,
) -> Option<usize> {
    let members = core::mem::size_of::<MemberIdentity>().checked_mul(plan.members().len())?;
    let identity_bytes = plan.members().iter().try_fold(0usize, |bytes, member| {
        bytes.checked_add(member.group_instance_id().len())
    })?;
    core::mem::size_of::<LeaveGroupRequest>()
        .checked_add(plan.group_id().len())
        .and_then(|bytes| bytes.checked_add(members))
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .and_then(|bytes| {
            let reason_bytes = plan.reason().map_or(Some(0usize), |reason| {
                reason.len().checked_mul(plan.members().len())
            })?;
            bytes.checked_add(reason_bytes)
        })
}
