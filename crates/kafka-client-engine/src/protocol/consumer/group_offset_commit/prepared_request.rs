//! Pre-core construction and exact charging of one generated commit request.

use kafka_client_core::{GroupCheckpoint, PartitionIndex, TopicId};
use kafka_wire::{
    OffsetCommitRequest, RetainedSize,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use kafka_wire_core::StrBytes;

use super::{ClassicGroupCommitSession, GroupOffsetCommitTopicName};

/// Pre-core request construction failure; no operation has been accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitRequestPreparationError {
    Allocation,
    UnknownTopic(TopicId),
    TopicCount,
    PartitionOutOfRange {
        topic_id: TopicId,
        partition: PartitionIndex,
    },
    ClassicGenerationOutOfRange,
}

/// Linear generated request built and charged before deterministic admission.
#[must_use = "the generated request must be submitted or deliberately released"]
pub(crate) struct PreparedGroupOffsetCommitRequest {
    request: OffsetCommitRequest,
    retained_bytes: usize,
}

impl PreparedGroupOffsetCommitRequest {
    pub(crate) fn try_new(
        session: &ClassicGroupCommitSession,
        checkpoint: &GroupCheckpoint,
        topic_names: &[GroupOffsetCommitTopicName],
    ) -> Result<Self, GroupOffsetCommitRequestPreparationError> {
        let topic_count = distinct_topic_count(checkpoint);
        let mut topics = Vec::new();
        topics
            .try_reserve_exact(topic_count)
            .map_err(|_error| GroupOffsetCommitRequestPreparationError::Allocation)?;
        for topic_name in topic_names {
            let entries = checkpoint
                .entries()
                .iter()
                .filter(|entry| entry.topic_id() == topic_name.topic_id);
            let partition_count = entries.clone().count();
            if partition_count == 0 {
                continue;
            }
            let mut partitions = Vec::new();
            partitions
                .try_reserve_exact(partition_count)
                .map_err(|_error| GroupOffsetCommitRequestPreparationError::Allocation)?;
            for entry in entries {
                let mut partition = OffsetCommitRequestPartition::default();
                partition.partition_index =
                    i32::try_from(entry.partition().get()).map_err(|_error| {
                        GroupOffsetCommitRequestPreparationError::PartitionOutOfRange {
                            topic_id: entry.topic_id(),
                            partition: entry.partition(),
                        }
                    })?;
                partition.committed_offset = entry.next_offset();
                partition.committed_leader_epoch = entry.leader_epoch().unwrap_or(-1);
                partition.committed_metadata = Some(StrBytes::default());
                partitions.push(partition);
            }
            let mut topic = OffsetCommitRequestTopic::default();
            topic.name = try_string(topic_name.name.as_ref())?;
            topic.partitions = partitions;
            topics.push(topic);
        }
        if topics.len() != topic_count {
            let missing = checkpoint
                .entries()
                .iter()
                .find(|entry| {
                    !topic_names
                        .iter()
                        .any(|topic| topic.topic_id == entry.topic_id())
                })
                .map(|entry| entry.topic_id());
            return Err(match missing {
                Some(topic_id) => GroupOffsetCommitRequestPreparationError::UnknownTopic(topic_id),
                None => GroupOffsetCommitRequestPreparationError::TopicCount,
            });
        }
        let mut request = OffsetCommitRequest::default();
        request.group_id = try_string(session.group.as_ref())?;
        request.generation_id_or_member_epoch =
            i32::try_from(session.classic_generation).map_err(|_error| {
                GroupOffsetCommitRequestPreparationError::ClassicGenerationOutOfRange
            })?;
        request.member_id = try_string(session.member.as_ref())?;
        request.group_instance_id = session
            .group_instance_id
            .as_ref()
            .map(|identity| try_string(identity.as_ref()))
            .transpose()?;
        request.retention_time_ms = -1;
        request.topics = topics;
        let retained_bytes = request.retained_size().heap_bytes();
        Ok(Self {
            request,
            retained_bytes,
        })
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn into_generated_offset_commit_request(self) -> OffsetCommitRequest {
        self.request
    }

    #[cfg(test)]
    pub(crate) fn from_request_for_test(request: OffsetCommitRequest) -> Self {
        let retained_bytes = request.retained_size().heap_bytes();
        Self {
            request,
            retained_bytes,
        }
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &OffsetCommitRequest {
        &self.request
    }
}

fn distinct_topic_count(checkpoint: &GroupCheckpoint) -> usize {
    checkpoint
        .entries()
        .iter()
        .enumerate()
        .filter(|(index, entry)| {
            *index == 0 || checkpoint.entries()[*index - 1].topic_id() != entry.topic_id()
        })
        .count()
}

fn try_string(value: &str) -> Result<StrBytes, GroupOffsetCommitRequestPreparationError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_error| GroupOffsetCommitRequestPreparationError::Allocation)?;
    owned.push_str(value);
    Ok(owned.into())
}
