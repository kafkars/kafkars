//! Bounded lifecycle topic identities and ordered core facts for direct assignment.

mod prepared;
#[cfg(test)]
mod prepared_test;

use kafka_client_core::{AssignedPartition, TopicId};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) use super::assigned_host::AssignedPartitionInput;
pub(super) use prepared::PreparedAssignedTopicsReplacement;

/// Bounds topic identities, retained names, and one current assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "each field names one explicit maximum resource dimension"
)]
pub(super) struct AssignedTopicLimits {
    max_retained_topics: usize,
    max_partitions: usize,
    max_topic_name_bytes: usize,
    max_retained_name_bytes: usize,
}

impl AssignedTopicLimits {
    pub(super) const fn new(
        max_retained_topics: usize,
        max_partitions: usize,
        max_topic_name_bytes: usize,
        max_retained_name_bytes: usize,
    ) -> Self {
        Self {
            max_retained_topics,
            max_partitions,
            max_topic_name_bytes,
            max_retained_name_bytes,
        }
    }

    pub(super) const fn max_partitions(self) -> usize {
        self.max_partitions
    }
}

/// Failure to retain or replace a bounded, identity-safe assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedTopicsError {
    PartitionCapacity { actual: usize, limit: usize },
    RetainedTopicCapacity { actual: usize, limit: usize },
    TopicNameBytes { actual: usize, limit: usize },
    RetainedNameBytes { actual: usize, limit: usize },
    RetainedNameBytesOverflow,
    RetainedTopicCountOverflow,
    TopicIdentityExhausted,
    UnknownTopic(TopicId),
}

/// Sole owner of retained names, stable identities, and the current assignment.
#[derive(Debug)]
pub(super) struct AssignedTopics {
    limits: AssignedTopicLimits,
    next_topic_id: Option<TopicId>,
    retained_name_bytes: usize,
    by_name: BTreeMap<Arc<str>, TopicId>,
    by_id: BTreeMap<TopicId, Arc<str>>,
    partitions: Vec<AssignedPartition>,
}

impl AssignedTopics {
    pub(super) const fn new(limits: AssignedTopicLimits) -> Self {
        Self {
            limits,
            next_topic_id: Some(TopicId::from_raw(1)),
            retained_name_bytes: 0,
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
            partitions: Vec::new(),
        }
    }

    pub(super) fn prepare_replacement(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
    ) -> Result<PreparedAssignedTopicsReplacement<'_>, AssignedTopicsError> {
        PreparedAssignedTopicsReplacement::prepare(self, entries)
    }

    fn install_replacement(
        &mut self,
        staged_names: BTreeMap<Arc<str>, TopicId>,
        staged_next_topic_id: Option<TopicId>,
        staged_name_bytes: usize,
        staged_partitions: Vec<AssignedPartition>,
    ) {
        for (name, topic_id) in staged_names {
            self.by_name.insert(Arc::clone(&name), topic_id);
            self.by_id.insert(topic_id, name);
        }
        self.next_topic_id = staged_next_topic_id;
        self.retained_name_bytes = staged_name_bytes;
        self.partitions = staged_partitions;
    }

    pub(super) fn partitions(&self) -> &[AssignedPartition] {
        &self.partitions
    }

    pub(super) fn retained_topic_count(&self) -> usize {
        self.by_name.len()
    }

    pub(super) const fn retained_name_bytes(&self) -> usize {
        self.retained_name_bytes
    }

    pub(super) fn name(&self, topic_id: TopicId) -> Result<&Arc<str>, AssignedTopicsError> {
        self.by_id
            .get(&topic_id)
            .ok_or(AssignedTopicsError::UnknownTopic(topic_id))
    }

    #[cfg(test)]
    pub(super) fn from_initial_for_test(
        entries: Vec<AssignedPartitionInput>,
        limits: AssignedTopicLimits,
    ) -> Result<Self, AssignedTopicsError> {
        let mut owner = Self::new(limits);
        owner.prepare_replacement(entries)?.commit();
        Ok(owner)
    }

    #[cfg(test)]
    pub(super) fn from_initial_with_next_for_test(
        entries: Vec<AssignedPartitionInput>,
        limits: AssignedTopicLimits,
        next_topic_id: Option<TopicId>,
    ) -> Result<Self, AssignedTopicsError> {
        let mut owner = Self {
            next_topic_id,
            ..Self::new(limits)
        };
        owner.prepare_replacement(entries)?.commit();
        Ok(owner)
    }

    #[cfg(test)]
    pub(super) const fn next_topic_id_for_test(&self) -> Option<TopicId> {
        self.next_topic_id
    }
}
