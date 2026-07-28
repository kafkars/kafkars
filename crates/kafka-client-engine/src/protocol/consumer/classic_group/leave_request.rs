//! Generated `LeaveGroup` construction for dynamic and opt-in static classic members.

use kafka_wire::{LeaveGroupRequest, leave_group_request::MemberIdentity};
use kafka_wire_core::StrBytes;

use super::validation::valid_kafka_string;

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicLeaveGroupRequestFailure {
    GroupName,
    MemberId,
    GroupInstanceId,
}

/// Linear ownership of one validated generated classic `LeaveGroup` request.
#[must_use = "a prepared LeaveGroup request must be submitted or deliberately released"]
pub(crate) struct PreparedClassicLeaveGroupRequest {
    request: LeaveGroupRequest,
}

impl PreparedClassicLeaveGroupRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_leave_group_request(self) -> LeaveGroupRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &LeaveGroupRequest {
        &self.request
    }
}

/// Builds one v0-v2-compatible dynamic-member `LeaveGroup` request.
pub(crate) fn classic_leave_group_request(
    group: &str,
    member: &str,
) -> Result<PreparedClassicLeaveGroupRequest, ClassicLeaveGroupRequestFailure> {
    classic_leave_group_request_with_instance(group, member, None)
}

/// Builds one exact member departure with an optional static identity.
pub(crate) fn classic_leave_group_request_with_instance(
    group: &str,
    member: &str,
    group_instance_id: Option<&str>,
) -> Result<PreparedClassicLeaveGroupRequest, ClassicLeaveGroupRequestFailure> {
    if !valid_kafka_string(group) {
        return Err(ClassicLeaveGroupRequestFailure::GroupName);
    }
    if !valid_kafka_string(member) {
        return Err(ClassicLeaveGroupRequestFailure::MemberId);
    }
    if group_instance_id.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicLeaveGroupRequestFailure::GroupInstanceId);
    }

    let mut request = LeaveGroupRequest::default();
    request.group_id = group.into();
    if let Some(group_instance_id) = group_instance_id {
        request.member_id = StrBytes::default();
        let mut identity = MemberIdentity::default();
        identity.member_id = member.into();
        identity.group_instance_id = Some(group_instance_id.into());
        request.members.push(identity);
    } else {
        request.member_id = member.into();
        request.members.clear();
    }
    Ok(PreparedClassicLeaveGroupRequest { request })
}
