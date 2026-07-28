//! Refcounted topic names and checked, never-reused deterministic identities.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::TopicId;

use super::ProducerStoreError;

#[derive(Debug)]
struct TopicEntry {
    id: TopicId,
    references: usize,
}

/// Stable identity owner for names retained by active producer records.
#[derive(Debug)]
pub(super) struct TopicCatalog {
    max_entries: usize,
    max_bytes: usize,
    retained_bytes: usize,
    next_topic_id: Option<TopicId>,
    by_name: BTreeMap<Arc<str>, TopicEntry>,
    by_id: BTreeMap<TopicId, Arc<str>>,
}

impl TopicCatalog {
    pub(super) const fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
            retained_bytes: 0,
            next_topic_id: Some(TopicId::from_raw(1)),
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
        }
    }

    pub(super) fn acquire(&mut self, name: Arc<str>) -> Result<TopicId, ProducerStoreError> {
        if let Some(entry) = self.by_name.get_mut(&name) {
            let Some(references) = entry.references.checked_add(1) else {
                return Err(ProducerStoreError::RetainedSizeOverflow);
            };
            entry.references = references;
            return Ok(entry.id);
        }
        if self.by_name.len() >= self.max_entries {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        }
        let retained_bytes = self
            .retained_bytes
            .checked_add(name.len())
            .ok_or(ProducerStoreError::RetainedSizeOverflow)?;
        if retained_bytes > self.max_bytes {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        }
        let Some(id) = self.next_topic_id else {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        };
        if self.by_id.contains_key(&id) {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        }
        self.next_topic_id = id.get().checked_add(1).map(TopicId::from_raw);
        self.retained_bytes = retained_bytes;
        self.by_name
            .insert(Arc::clone(&name), TopicEntry { id, references: 1 });
        self.by_id.insert(id, name);
        Ok(id)
    }

    pub(super) fn release(&mut self, id: TopicId) -> Result<(), ProducerStoreError> {
        let name = self
            .by_id
            .get(&id)
            .ok_or(ProducerStoreError::UnknownTopic)?;
        let entry = self
            .by_name
            .get_mut(name)
            .ok_or(ProducerStoreError::UnknownTopic)?;
        entry.references = entry
            .references
            .checked_sub(1)
            .ok_or(ProducerStoreError::InvalidPayloadState)?;
        Ok(())
    }

    pub(super) fn name(&self, id: TopicId) -> Result<&Arc<str>, ProducerStoreError> {
        self.by_id.get(&id).ok_or(ProducerStoreError::UnknownTopic)
    }

    pub(super) fn clear_terminal(&mut self) {
        self.by_name.clear();
        self.by_id.clear();
        self.retained_bytes = 0;
    }

    pub(super) fn len(&self) -> usize {
        self.by_name
            .values()
            .filter(|entry| entry.references != 0)
            .count()
    }
}
