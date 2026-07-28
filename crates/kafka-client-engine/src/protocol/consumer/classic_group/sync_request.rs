//! Generated `SyncGroup` construction from one complete core-owned slot plan.

use kafka_client_core::{ClassicAssignmentPlan, ClassicGeneration, JoinedMemberSlot, TopicId};
use kafka_wire::{SyncGroupRequest, sync_group_request::SyncGroupRequestAssignment};
use kafka_wire_core::EncodeError;

use super::{
    ClassicSyncMember, ClassicSyncTopic,
    sync_assignment::materialize_assignment,
    validation::{MAX_MEMBERS, MAX_TOPICS, valid_kafka_string},
};

mod plan_validation;
#[cfg(test)]
mod plan_validation_test;

use plan_validation::{member_for_slot, validate_members, validate_topics};

/// Local plan-correlation or encoding failure before driver ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicSyncRequestFailure {
    InvalidGroup,
    InvalidMember,
    InvalidGroupInstance,
    MemberCount { actual: usize, limit: usize },
    TopicCount { actual: usize, limit: usize },
    InvalidMappedMember,
    InvalidMappedTopic,
    DuplicateMemberSlot(JoinedMemberSlot),
    DuplicateMember,
    DuplicateTopicId(TopicId),
    DuplicateTopic,
    MissingMember(JoinedMemberSlot),
    UnexpectedMember(JoinedMemberSlot),
    MissingTopic(TopicId),
    LocalMemberMissing,
    PartitionOutOfRange(u32),
    Allocation,
    Encode(EncodeError),
}

/// Linear ownership of one validated generated classic Sync request.
#[must_use = "a prepared classic Sync request must be submitted or deliberately released"]
pub(crate) struct PreparedClassicSyncGroupRequest {
    request: SyncGroupRequest,
}

impl PreparedClassicSyncGroupRequest {
    /// Transfers the generated request at the tracked driver-call boundary.
    pub(crate) fn into_generated_sync_group_request(self) -> SyncGroupRequest {
        self.request
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &SyncGroupRequest {
        &self.request
    }
}

/// Builds one v0-v2-compatible dynamic Range `SyncGroup` request.
pub(crate) fn classic_sync_group_request(
    group: &str,
    local_member: &str,
    generation: ClassicGeneration,
    plan: ClassicAssignmentPlan,
    members: &[ClassicSyncMember],
    topics: &[ClassicSyncTopic],
) -> Result<PreparedClassicSyncGroupRequest, ClassicSyncRequestFailure> {
    classic_sync_group_request_with_instance(
        group,
        local_member,
        None,
        generation,
        plan,
        members,
        topics,
    )
}

/// Builds one complete-plan request with an optional static identity.
pub(crate) fn classic_sync_group_request_with_instance(
    group: &str,
    local_member: &str,
    group_instance_id: Option<&str>,
    generation: ClassicGeneration,
    plan: ClassicAssignmentPlan,
    members: &[ClassicSyncMember],
    topics: &[ClassicSyncTopic],
) -> Result<PreparedClassicSyncGroupRequest, ClassicSyncRequestFailure> {
    validate_inputs(
        group,
        local_member,
        group_instance_id,
        plan.entries(),
        members,
        topics,
    )?;
    let mut assignments = Vec::new();
    assignments
        .try_reserve_exact(plan.entries().len())
        .map_err(|_error| ClassicSyncRequestFailure::Allocation)?;
    for assignment in plan.into_sync_assignments() {
        let member = member_for_slot(members, assignment.slot())?;
        let wire_assignment = materialize_assignment(assignment.partitions(), topics)?;
        let mut assignment = SyncGroupRequestAssignment::default();
        assignment.member_id = member.into();
        assignment.assignment = wire_assignment;
        assignments.push(assignment);
    }
    Ok(PreparedClassicSyncGroupRequest {
        request: sync_request(
            group,
            local_member,
            group_instance_id,
            generation,
            assignments,
        ),
    })
}

/// Builds the exact empty-plan request emitted for a dynamic follower.
pub(crate) fn classic_follower_sync_group_request(
    group: &str,
    local_member: &str,
    generation: ClassicGeneration,
) -> Result<PreparedClassicSyncGroupRequest, ClassicSyncRequestFailure> {
    classic_follower_sync_group_request_with_instance(group, local_member, None, generation)
}

/// Builds an empty-plan follower request with an optional static identity.
pub(crate) fn classic_follower_sync_group_request_with_instance(
    group: &str,
    local_member: &str,
    group_instance_id: Option<&str>,
    generation: ClassicGeneration,
) -> Result<PreparedClassicSyncGroupRequest, ClassicSyncRequestFailure> {
    validate_inputs(group, local_member, group_instance_id, &[], &[], &[])?;
    Ok(PreparedClassicSyncGroupRequest {
        request: sync_request(
            group,
            local_member,
            group_instance_id,
            generation,
            Vec::new(),
        ),
    })
}

fn sync_request(
    group: &str,
    local_member: &str,
    group_instance_id: Option<&str>,
    generation: ClassicGeneration,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    let mut request = SyncGroupRequest::default();
    request.group_id = group.into();
    request.generation_id = generation.get();
    request.member_id = local_member.into();
    request.group_instance_id = group_instance_id.map(Into::into);
    request.protocol_type = None;
    request.protocol_name = None;
    request.assignments = assignments;
    request
}

fn validate_inputs(
    group: &str,
    local_member: &str,
    group_instance_id: Option<&str>,
    plan: &[kafka_client_core::ClassicMemberAssignment],
    members: &[ClassicSyncMember],
    topics: &[ClassicSyncTopic],
) -> Result<(), ClassicSyncRequestFailure> {
    if !valid_kafka_string(group) {
        return Err(ClassicSyncRequestFailure::InvalidGroup);
    }
    if !valid_kafka_string(local_member) {
        return Err(ClassicSyncRequestFailure::InvalidMember);
    }
    if group_instance_id.is_some_and(|value| !valid_kafka_string(value)) {
        return Err(ClassicSyncRequestFailure::InvalidGroupInstance);
    }
    if members.len() > MAX_MEMBERS {
        return Err(ClassicSyncRequestFailure::MemberCount {
            actual: members.len(),
            limit: MAX_MEMBERS,
        });
    }
    if topics.len() > MAX_TOPICS {
        return Err(ClassicSyncRequestFailure::TopicCount {
            actual: topics.len(),
            limit: MAX_TOPICS,
        });
    }
    validate_members(plan, members, local_member)?;
    validate_topics(topics)
}
