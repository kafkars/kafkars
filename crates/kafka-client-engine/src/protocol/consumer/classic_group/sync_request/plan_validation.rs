//! Exact member-slot and topic-identity validation for one Sync plan.

use kafka_client_core::{ClassicMemberAssignment, JoinedMemberSlot};

use super::{ClassicSyncMember, ClassicSyncRequestFailure, ClassicSyncTopic};
use crate::protocol::consumer::classic_group::validation::{valid_kafka_string, valid_topic};

pub(super) fn validate_members(
    plan: &[ClassicMemberAssignment],
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

pub(super) fn validate_topics(
    topics: &[ClassicSyncTopic],
) -> Result<(), ClassicSyncRequestFailure> {
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

pub(super) fn member_for_slot(
    members: &[ClassicSyncMember],
    slot: JoinedMemberSlot,
) -> Result<&str, ClassicSyncRequestFailure> {
    members
        .iter()
        .find(|member| member.slot() == slot)
        .map(ClassicSyncMember::member)
        .ok_or(ClassicSyncRequestFailure::MissingMember(slot))
}
