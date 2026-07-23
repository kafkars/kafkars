//! Refcounted topic names and checked, never-reused deterministic identities.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::TopicId;

use super::ProducerStoreError;

#[derive(Clone, Copy, Debug)]
struct TopicEntry {
    id: TopicId,
    references: usize,
}

/// Stable identity owner for names retained by active producer records.
#[derive(Debug)]
pub(super) struct TopicCatalog {
    next_topic_id: Option<TopicId>,
    by_name: BTreeMap<Arc<str>, TopicEntry>,
    by_id: BTreeMap<TopicId, Arc<str>>,
}

impl TopicCatalog {
    pub(super) const fn new() -> Self {
        Self {
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
        let Some(id) = self.next_topic_id else {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        };
        if self.by_id.contains_key(&id) {
            return Err(ProducerStoreError::TopicIdentityExhausted);
        }
        self.next_topic_id = id.get().checked_add(1).map(TopicId::from_raw);
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
        if entry.references > 1 {
            entry.references -= 1;
            return Ok(());
        }
        let name = self
            .by_id
            .remove(&id)
            .ok_or(ProducerStoreError::UnknownTopic)?;
        self.by_name.remove(&name);
        Ok(())
    }

    pub(super) fn name(&self, id: TopicId) -> Result<&Arc<str>, ProducerStoreError> {
        self.by_id.get(&id).ok_or(ProducerStoreError::UnknownTopic)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_name.len()
    }
}
