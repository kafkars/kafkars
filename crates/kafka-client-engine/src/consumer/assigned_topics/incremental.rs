//! Linear topic-catalog preparation for incremental direct-assignment changes.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{AssignedPartition, AssignedTopicPartition, TopicId};

use super::{AssignedPartitionInput, AssignedTopics, AssignedTopicsError};
use crate::consumer::assigned_host::AssignedConsumerPartition;

/// Candidate additions retaining both new core facts and a complete catalog install.
#[must_use = "prepared additions must be committed only after core accepts, or dropped"]
pub(in crate::consumer) struct PreparedAssignedTopicsAddition<'a> {
    owner: &'a mut AssignedTopics,
    staged_names: BTreeMap<Arc<str>, TopicId>,
    staged_next_topic_id: Option<TopicId>,
    staged_name_bytes: usize,
    added: Vec<AssignedPartition>,
    assignment: Vec<AssignedPartition>,
}

impl<'a> PreparedAssignedTopicsAddition<'a> {
    pub(in crate::consumer) fn prepare(
        owner: &'a mut AssignedTopics,
        entries: Vec<AssignedPartitionInput>,
    ) -> Result<Self, AssignedTopicsError> {
        let final_count = owner
            .partitions()
            .len()
            .checked_add(entries.len())
            .ok_or(AssignedTopicsError::RetainedTopicCountOverflow)?;
        if final_count > owner.limits.max_partitions {
            return Err(AssignedTopicsError::PartitionCapacity {
                actual: final_count,
                limit: owner.limits.max_partitions,
            });
        }
        let mut prepared = Self {
            staged_next_topic_id: owner.next_topic_id(),
            staged_name_bytes: owner.retained_name_bytes(),
            staged_names: BTreeMap::new(),
            added: Vec::new(),
            assignment: Vec::new(),
            owner,
        };
        prepared
            .added
            .try_reserve_exact(entries.len())
            .map_err(|_error| AssignedTopicsError::Allocation)?;
        prepared
            .assignment
            .try_reserve_exact(final_count)
            .map_err(|_error| AssignedTopicsError::Allocation)?;
        prepared
            .assignment
            .extend_from_slice(prepared.owner.partitions());
        for entry in entries {
            let partition_index = entry.partition_index();
            let topic_id = prepared.stage_topic(entry.topic)?;
            let partition = AssignedPartition::new(
                AssignedTopicPartition::new(topic_id, partition_index),
                entry.start,
            );
            prepared.added.push(partition);
            prepared.assignment.push(partition);
        }
        Ok(prepared)
    }

    pub(in crate::consumer) fn added(&self) -> &[AssignedPartition] {
        &self.added
    }

    pub(in crate::consumer) fn commit(self) {
        let Self {
            owner,
            staged_names,
            staged_next_topic_id,
            staged_name_bytes,
            added: _,
            assignment,
        } = self;
        owner.install_replacement(
            staged_names,
            staged_next_topic_id,
            staged_name_bytes,
            assignment,
        );
    }

    fn stage_topic(&mut self, topic: Arc<str>) -> Result<TopicId, AssignedTopicsError> {
        if let Some(topic_id) = self.owner.retained_topic_id(&topic) {
            return Ok(topic_id);
        }
        if let Some(topic_id) = self.staged_names.get(&topic) {
            return Ok(*topic_id);
        }
        if topic.len() > self.owner.limits.max_topic_name_bytes {
            return Err(AssignedTopicsError::TopicNameBytes {
                actual: topic.len(),
                limit: self.owner.limits.max_topic_name_bytes,
            });
        }
        let actual_topics = self
            .owner
            .retained_topic_count()
            .checked_add(self.staged_names.len())
            .and_then(|value| value.checked_add(1))
            .ok_or(AssignedTopicsError::RetainedTopicCountOverflow)?;
        if actual_topics > self.owner.limits.max_retained_topics {
            return Err(AssignedTopicsError::RetainedTopicCapacity {
                actual: actual_topics,
                limit: self.owner.limits.max_retained_topics,
            });
        }
        let actual_name_bytes = self
            .staged_name_bytes
            .checked_add(topic.len())
            .ok_or(AssignedTopicsError::RetainedNameBytesOverflow)?;
        if actual_name_bytes > self.owner.limits.max_retained_name_bytes {
            return Err(AssignedTopicsError::RetainedNameBytes {
                actual: actual_name_bytes,
                limit: self.owner.limits.max_retained_name_bytes,
            });
        }
        let topic_id = self
            .staged_next_topic_id
            .ok_or(AssignedTopicsError::TopicIdentityExhausted)?;
        if self.owner.topic_id_is_retained(topic_id) {
            return Err(AssignedTopicsError::TopicIdentityExhausted);
        }
        self.staged_next_topic_id = topic_id.get().checked_add(1).map(TopicId::from_raw);
        self.staged_name_bytes = actual_name_bytes;
        self.staged_names.insert(topic, topic_id);
        Ok(topic_id)
    }
}

/// Candidate removals retaining exact targets and survivor ordering until commit.
#[must_use = "prepared removals must be committed only after core accepts, or dropped"]
pub(in crate::consumer) struct PreparedAssignedTopicsRemoval<'a> {
    owner: &'a mut AssignedTopics,
    removed: Vec<AssignedTopicPartition>,
    assignment: Vec<AssignedPartition>,
}

impl<'a> PreparedAssignedTopicsRemoval<'a> {
    pub(in crate::consumer) fn prepare(
        owner: &'a mut AssignedTopics,
        entries: &[AssignedConsumerPartition],
    ) -> Result<Self, AssignedTopicsError> {
        let mut removed = Vec::new();
        removed
            .try_reserve_exact(entries.len())
            .map_err(|_error| AssignedTopicsError::Allocation)?;
        for entry in entries {
            let topic_id = owner
                .retained_topic_id(&entry.topic)
                .ok_or(AssignedTopicsError::UnknownTopicName)?;
            removed.push(AssignedTopicPartition::new(
                topic_id,
                entry.partition_index(),
            ));
        }
        let mut assignment = Vec::new();
        assignment
            .try_reserve_exact(owner.partitions().len())
            .map_err(|_error| AssignedTopicsError::Allocation)?;
        assignment.extend(
            owner
                .partitions()
                .iter()
                .copied()
                .filter(|present| !removed.contains(&present.partition())),
        );
        Ok(Self {
            owner,
            removed,
            assignment,
        })
    }

    pub(in crate::consumer) fn removed(&self) -> &[AssignedTopicPartition] {
        &self.removed
    }

    pub(in crate::consumer) fn commit(self) {
        self.owner.install_partitions(self.assignment);
    }
}
