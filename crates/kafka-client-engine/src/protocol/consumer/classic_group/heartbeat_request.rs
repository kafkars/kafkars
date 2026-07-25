//! Generated `Heartbeat` construction for one dynamic classic-group member.

use kafka_client_core::ClassicGeneration;
use kafka_wire::HeartbeatRequest;

use super::validation::valid_kafka_string;

/// Local request-shape failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicHeartbeatRequestFailure {
    InvalidGroup,
    InvalidMember,
}

/// Linear ownership of one validated generated classic Heartbeat request.
#[must_use = "a prepared classic Heartbeat request must be submitted or deliberately released"]
pub(crate) struct PreparedClassicHeartbeatRequest {
    request: HeartbeatRequest,
}

impl PreparedClassicHeartbeatRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_heartbeat_request(self) -> HeartbeatRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &HeartbeatRequest {
        &self.request
    }
}

/// Builds one v0-v2-compatible dynamic-member `Heartbeat` request.
pub(crate) fn classic_heartbeat_request(
    group: &str,
    member: &str,
    generation: ClassicGeneration,
) -> Result<PreparedClassicHeartbeatRequest, ClassicHeartbeatRequestFailure> {
    if !valid_kafka_string(group) {
        return Err(ClassicHeartbeatRequestFailure::InvalidGroup);
    }
    if !valid_kafka_string(member) {
        return Err(ClassicHeartbeatRequestFailure::InvalidMember);
    }
    let mut request = HeartbeatRequest::default();
    request.group_id = group.into();
    request.generation_id = generation.get();
    request.member_id = member.into();
    request.group_instance_id = None;
    Ok(PreparedClassicHeartbeatRequest { request })
}
