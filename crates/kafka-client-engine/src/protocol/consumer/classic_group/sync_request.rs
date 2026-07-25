//! Generated `SyncGroup` construction from one complete core-owned slot plan.

use kafka_client_core::{ClassicAssignmentPlan, ClassicGeneration, JoinedMemberSlot, TopicId};
use kafka_wire::{SyncGroupRequest, sync_group_request::SyncGroupRequestAssignment};
use kafka_wire_core::EncodeError;

use super::{
    ClassicSyncMember, ClassicSyncTopic,
    sync_assignment::materialize_assignment,
    validation::{MAX_MEMBERS, MAX_TOPICS, valid_kafka_string, valid_topic},
};

/// Local plan-correlation or encoding failure before driver ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClassicSyncRequestFailure {
    InvalidGroup,
    InvalidMember,
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
    validate_inputs(group, local_member, plan.entries(), members, topics)?;
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
        request: sync_request(group, local_member, generation, assignments),
    })
}

/// Builds the exact empty-plan request emitted for a dynamic follower.
pub(crate) fn classic_follower_sync_group_request(
    group: &str,
    local_member: &str,
    generation: ClassicGeneration,
) -> Result<PreparedClassicSyncGroupRequest, ClassicSyncRequestFailure> {
    validate_inputs(group, local_member, &[], &[], &[])?;
    Ok(PreparedClassicSyncGroupRequest {
        request: sync_request(group, local_member, generation, Vec::new()),
    })
}

fn sync_request(
    group: &str,
    local_member: &str,
    generation: ClassicGeneration,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    let mut request = SyncGroupRequest::default();
    request.group_id = group.into();
    request.generation_id = generation.get();
    request.member_id = local_member.into();
    request.group_instance_id = None;
    request.protocol_type = None;
    request.protocol_name = None;
    request.assignments = assignments;
    request
}

fn validate_inputs(
    group: &str,
    local_member: &str,
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

fn validate_members(
    plan: &[kafka_client_core::ClassicMemberAssignment],
    members: &[ClassicSyncMember],
    local_member: &str,
) -> Result<(), ClassicSyncRequestFailure> {
    if plan.is_empty() {
        return if members.is_empty() {
            Ok(())
        } else {
            Err(ClassicSyncRequestFailure::UnexpectedMember(
                members[0].slot(),
            ))
        };
    }
    for (index, member) in members.iter().enumerate() {
        if !valid_kafka_string(member.member()) {
            return Err(ClassicSyncRequestFailure::InvalidMappedMember);
        }
        if members[..index]
            .iter()
            .any(|prior| prior.slot() == member.slot())
        {
            return Err(ClassicSyncRequestFailure::DuplicateMemberSlot(
                member.slot(),
            ));
        }
        if members[..index]
            .iter()
            .any(|prior| prior.member() == member.member())
        {
            return Err(ClassicSyncRequestFailure::DuplicateMember);
        }
        if !plan.iter().any(|entry| entry.slot() == member.slot()) {
            return Err(ClassicSyncRequestFailure::UnexpectedMember(member.slot()));
        }
    }
    for entry in plan {
        if !members.iter().any(|member| member.slot() == entry.slot()) {
            return Err(ClassicSyncRequestFailure::MissingMember(entry.slot()));
        }
    }
    if !members.iter().any(|member| member.member() == local_member) {
        return Err(ClassicSyncRequestFailure::LocalMemberMissing);
    }
    Ok(())
}

fn validate_topics(topics: &[ClassicSyncTopic]) -> Result<(), ClassicSyncRequestFailure> {
    for (index, topic) in topics.iter().enumerate() {
        if !valid_topic(topic.topic()) {
            return Err(ClassicSyncRequestFailure::InvalidMappedTopic);
        }
        if topics[..index]
            .iter()
            .any(|prior| prior.topic_id() == topic.topic_id())
        {
            return Err(ClassicSyncRequestFailure::DuplicateTopicId(
                topic.topic_id(),
            ));
        }
        if topics[..index]
            .iter()
            .any(|prior| prior.topic() == topic.topic())
        {
            return Err(ClassicSyncRequestFailure::DuplicateTopic);
        }
    }
    Ok(())
}

fn member_for_slot(
    members: &[ClassicSyncMember],
    slot: JoinedMemberSlot,
) -> Result<&str, ClassicSyncRequestFailure> {
    members
        .iter()
        .find(|member| member.slot() == slot)
        .map(ClassicSyncMember::member)
        .ok_or(ClassicSyncRequestFailure::MissingMember(slot))
}
