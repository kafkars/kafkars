//! Producer-lifetime canonical topic identities with atomic staged insertion.

use std::sync::Arc;

use kafka_client_core::TopicId;

#[derive(Debug)]
struct TransactionTopicEntry {
    name: Arc<str>,
    id: TopicId,
}

#[derive(Debug)]
struct StagedTransactionTopicCatalog {
    entries: Vec<TransactionTopicEntry>,
    next_topic_id: Option<TopicId>,
    retained_topic_bytes: usize,
}

/// Failure before a canonical topic acquires producer-lifetime identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionTopicCatalogError {
    RetainedTopicCapacity { actual: usize, limit: usize },
    RetainedTopicBytes { actual: usize, limit: usize },
    RetainedTopicBytesOverflow,
    TopicIdentityExhausted,
    Allocation,
}

/// Prepared existing lookup or fully allocated first-topic insertion.
#[must_use = "prepared topic identity must commit only after send acceptance"]
#[derive(Debug)]
pub(super) struct PreparedTransactionTopic {
    topic_id: TopicId,
    staged: Option<StagedTransactionTopicCatalog>,
}

impl PreparedTransactionTopic {
    pub(super) const fn topic_id(&self) -> TopicId {
        self.topic_id
    }
}

/// Bounded canonical names and never-reused identities for one producer owner.
#[derive(Debug)]
pub(super) struct TransactionTopicCatalog {
    topic_capacity: usize,
    retained_topic_byte_limit: usize,
    next_topic_id: Option<TopicId>,
    retained_topic_bytes: usize,
    entries: Vec<TransactionTopicEntry>,
}

impl TransactionTopicCatalog {
    pub(super) const fn new(topic_capacity: usize, retained_topic_byte_limit: usize) -> Self {
        Self {
            topic_capacity,
            retained_topic_byte_limit,
            next_topic_id: Some(TopicId::from_raw(1)),
            retained_topic_bytes: 0,
            entries: Vec::new(),
        }
    }

    pub(super) fn prepare(
        &self,
        canonical_topic: &Arc<str>,
    ) -> Result<PreparedTransactionTopic, TransactionTopicCatalogError> {
        if let Some(topic_id) = self.topic_id(canonical_topic) {
            return Ok(PreparedTransactionTopic {
                topic_id,
                staged: None,
            });
        }
        let actual_topics = self.entries.len().saturating_add(1);
        if actual_topics > self.topic_capacity {
            return Err(TransactionTopicCatalogError::RetainedTopicCapacity {
                actual: actual_topics,
                limit: self.topic_capacity,
            });
        }
        let retained_topic_bytes = self
            .retained_topic_bytes
            .checked_add(canonical_topic.len())
            .ok_or(TransactionTopicCatalogError::RetainedTopicBytesOverflow)?;
        if retained_topic_bytes > self.retained_topic_byte_limit {
            return Err(TransactionTopicCatalogError::RetainedTopicBytes {
                actual: retained_topic_bytes,
                limit: self.retained_topic_byte_limit,
            });
        }
        let topic_id = self
            .next_topic_id
            .ok_or(TransactionTopicCatalogError::TopicIdentityExhausted)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(actual_topics)
            .map_err(|_error| TransactionTopicCatalogError::Allocation)?;
        entries.extend(self.entries.iter().map(|entry| TransactionTopicEntry {
            name: Arc::clone(&entry.name),
            id: entry.id,
        }));
        entries.push(TransactionTopicEntry {
            name: Arc::clone(canonical_topic),
            id: topic_id,
        });
        Ok(PreparedTransactionTopic {
            topic_id,
            staged: Some(StagedTransactionTopicCatalog {
                entries,
                next_topic_id: topic_id.get().checked_add(1).map(TopicId::from_raw),
                retained_topic_bytes,
            }),
        })
    }

    pub(super) fn commit(&mut self, prepared: PreparedTransactionTopic) {
        let Some(staged) = prepared.staged else {
            return;
        };
        self.entries = staged.entries;
        self.next_topic_id = staged.next_topic_id;
        self.retained_topic_bytes = staged.retained_topic_bytes;
    }

    pub(super) fn topic_id(&self, canonical_topic: &str) -> Option<TopicId> {
        self.entries
            .iter()
            .find(|entry| entry.name.as_ref() == canonical_topic)
            .map(|entry| entry.id)
    }

    #[cfg(test)]
    pub(super) fn retained_topic_count(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) const fn retained_topic_bytes(&self) -> usize {
        self.retained_topic_bytes
    }

    #[cfg(test)]
    pub(super) fn set_next_topic_id(&mut self, next_topic_id: Option<TopicId>) {
        self.next_topic_id = next_topic_id;
    }
}
