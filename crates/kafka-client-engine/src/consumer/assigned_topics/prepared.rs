//! Linear two-phase preparation of one bounded direct-assignment replacement.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::{AssignedPartition, AssignedTopicPartition, TopicId};

use super::{AssignedPartitionInput, AssignedTopics, AssignedTopicsError};

/// Candidate assignment facts that leave the catalog untouched until committed.
#[must_use = "a prepared assignment must be committed only after core accepts, or dropped"]
#[derive(Debug)]
pub(in crate::consumer) struct PreparedAssignedTopicsReplacement<'a> {
    owner: &'a mut AssignedTopics,
    staged_names: BTreeMap<Arc<str>, TopicId>,
    staged_next_topic_id: Option<TopicId>,
    staged_name_bytes: usize,
    partitions: Vec<AssignedPartition>,
}

impl<'a> PreparedAssignedTopicsReplacement<'a> {
    pub(in crate::consumer) fn prepare(
        owner: &'a mut AssignedTopics,
        entries: Vec<AssignedPartitionInput>,
    ) -> Result<Self, AssignedTopicsError> {
        if entries.len() > owner.limits.max_partitions {
            return Err(AssignedTopicsError::PartitionCapacity {
                actual: entries.len(),
                limit: owner.limits.max_partitions,
            });
        }
        let mut prepared = Self {
            staged_next_topic_id: owner.next_topic_id,
            staged_name_bytes: owner.retained_name_bytes,
            staged_names: BTreeMap::new(),
            partitions: Vec::with_capacity(entries.len()),
            owner,
        };
        for entry in entries {
            let topic_id = prepared.stage_topic(entry.topic)?;
            prepared.partitions.push(AssignedPartition::new(
                AssignedTopicPartition::new(topic_id, entry.partition),
                entry.start,
            ));
        }
        Ok(prepared)
    }

    pub(in crate::consumer) fn partitions(&self) -> &[AssignedPartition] {
        &self.partitions
    }

    pub(in crate::consumer) fn commit(self) {
        let Self {
            owner,
            staged_names,
            staged_next_topic_id,
            staged_name_bytes,
            partitions,
        } = self;
        owner.install_replacement(
            staged_names,
            staged_next_topic_id,
            staged_name_bytes,
            partitions,
        );
    }

    fn stage_topic(&mut self, topic: Arc<str>) -> Result<TopicId, AssignedTopicsError> {
        if let Some(topic_id) = self.owner.by_name.get(&topic) {
            return Ok(*topic_id);
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
            .by_name
            .len()
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
        if self.owner.by_id.contains_key(&topic_id) {
            return Err(AssignedTopicsError::TopicIdentityExhausted);
        }
        self.staged_next_topic_id = topic_id.get().checked_add(1).map(TopicId::from_raw);
        self.staged_name_bytes = actual_name_bytes;
        self.staged_names.insert(topic, topic_id);
        Ok(topic_id)
    }
}
