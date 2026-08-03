//! Exact engine-catalog spellings and protocol-aware group epoch facts.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupMemberEpoch, GroupId, MemberId, TopicId,
};

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

/// Protocol-aware payload for Kafka's shared group-epoch wire field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GroupOffsetCommitEpoch<T> {
    /// Classic-group generation carried by `OffsetCommit` v2 and later.
    Classic {
        /// Exact generation ID issued by `JoinGroup`.
        generation_id: T,
    },
    /// Consumer-group member epoch carried by `OffsetCommit` v9.
    Consumer {
        /// Exact member epoch issued by `ConsumerGroupHeartbeat`.
        member_epoch: T,
    },
}

impl GroupOffsetCommitEpoch<i64> {
    pub(super) fn try_narrow(self) -> Option<GroupOffsetCommitEpoch<i32>> {
        match self {
            Self::Classic { generation_id } => {
                let generation_id = i32::try_from(generation_id).ok()?;
                (generation_id >= 0).then_some(GroupOffsetCommitEpoch::Classic { generation_id })
            }
            Self::Consumer { member_epoch } => {
                let member_epoch = i32::try_from(member_epoch).ok()?;
                (member_epoch > 0).then_some(GroupOffsetCommitEpoch::Consumer { member_epoch })
            }
        }
    }
}

impl GroupOffsetCommitEpoch<i32> {
    pub(super) const fn generation_id_or_member_epoch(self) -> i32 {
        match self {
            Self::Classic { generation_id } => generation_id,
            Self::Consumer { member_epoch } => member_epoch,
        }
    }

    pub(super) const fn requires_consumer_group_version(self) -> bool {
        matches!(self, Self::Consumer { .. })
    }
}

/// Group session facts resolved by the engine catalog.
#[derive(Debug)]
pub(crate) struct ClassicGroupCommitSession {
    pub(super) group_id: GroupId,
    pub(super) group: Arc<str>,
    pub(super) member_id: MemberId,
    pub(super) member: Arc<str>,
    pub(super) group_instance_id: Option<Arc<str>>,
    pub(super) assignment_generation: AssignmentGeneration,
    pub(super) epoch: GroupOffsetCommitEpoch<i64>,
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
            group_instance_id: None,
            assignment_generation,
            epoch: GroupOffsetCommitEpoch::Classic {
                generation_id: classic_generation,
            },
        }
    }

    pub(crate) fn new_consumer(
        group_id: GroupId,
        group: Arc<str>,
        member_id: MemberId,
        member: Arc<str>,
        assignment_generation: AssignmentGeneration,
        member_epoch: ConsumerGroupMemberEpoch,
    ) -> Self {
        Self {
            group_id,
            group,
            member_id,
            member,
            group_instance_id: None,
            assignment_generation,
            epoch: GroupOffsetCommitEpoch::Consumer {
                member_epoch: i64::from(member_epoch.get()),
            },
        }
    }

    pub(crate) fn with_group_instance_id(mut self, group_instance_id: Option<Arc<str>>) -> Self {
        self.group_instance_id = group_instance_id;
        self
    }
}
