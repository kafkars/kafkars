//! Exact engine-catalog spellings and classic-group generation facts.

use std::sync::Arc;

use kafka_client_core::{AssignmentGeneration, GroupId, MemberId, TopicId};

/// Legacy Kafka string byte limit used by group and member identifiers.
pub(super) const MAX_GROUP_OFFSET_COMMIT_ID_BYTES: usize = i16::MAX as usize;
/// Kafka topic-name byte limit.
pub(super) const MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES: usize = 249;

/// Exact catalog spelling for one checkpoint topic identity.
#[derive(Debug)]
pub(crate) struct GroupOffsetCommitTopicName {
    pub(super) topic_id: TopicId,
    pub(super) name: Arc<str>,
}

impl GroupOffsetCommitTopicName {
    pub(crate) const fn new(topic_id: TopicId, name: Arc<str>) -> Self {
        Self { topic_id, name }
    }
}

/// Classic-group session facts resolved by the engine catalog.
#[derive(Debug)]
pub(crate) struct ClassicGroupCommitSession {
    pub(super) group_id: GroupId,
    pub(super) group: Arc<str>,
    pub(super) member_id: MemberId,
    pub(super) member: Arc<str>,
    pub(super) assignment_generation: AssignmentGeneration,
    pub(super) classic_generation: i64,
}

impl ClassicGroupCommitSession {
    pub(crate) fn new(
        group_id: GroupId,
        group: Arc<str>,
        member_id: MemberId,
        member: Arc<str>,
        assignment_generation: AssignmentGeneration,
        classic_generation: i64,
    ) -> Self {
        Self {
            group_id,
            group,
            member_id,
            member,
            assignment_generation,
            classic_generation,
        }
    }
}
