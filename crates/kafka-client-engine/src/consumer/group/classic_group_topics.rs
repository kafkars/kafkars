//! Candidate-local translation of bounded topic spellings to staged identities.

use std::{collections::BTreeMap, sync::Arc};

use kafka_client_core::TopicId;

use super::{
    classic_group_candidate::ClassicGroupCycleCandidateError,
    session_catalog::{
        GroupSessionCatalog, GroupSessionCatalogError, MAX_GROUP_SESSION_TOPIC_NAME_BYTES,
        MAX_GROUP_SESSION_TOPICS, validate_topic,
    },
};

pub(super) struct PreparedCycleTopics<'a> {
    catalog: &'a GroupSessionCatalog,
    pub(super) staged: BTreeMap<Arc<str>, TopicId>,
    pub(super) next_topic_id: Option<TopicId>,
    pub(super) retained_topic_name_bytes: usize,
}

impl<'a> PreparedCycleTopics<'a> {
    pub(super) fn new(catalog: &'a GroupSessionCatalog) -> Self {
        Self {
            catalog,
            staged: BTreeMap::new(),
            next_topic_id: catalog.next_topic_id,
            retained_topic_name_bytes: catalog.retained_topic_name_bytes,
        }
    }

    pub(super) fn translate_subscription(
        &mut self,
        topics: Vec<Arc<str>>,
    ) -> Result<Vec<TopicId>, ClassicGroupCycleCandidateError> {
        let mut translated = Vec::new();
        translated
            .try_reserve_exact(topics.len())
            .map_err(|_error| ClassicGroupCycleCandidateError::Allocation)?;
        for topic in topics {
            translated.push(self.stage_topic(topic)?);
        }
        translated.sort_unstable();
        if translated.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ClassicGroupCycleCandidateError::DuplicateTopic);
        }
        Ok(translated)
    }

    fn stage_topic(&mut self, topic: Arc<str>) -> Result<TopicId, ClassicGroupCycleCandidateError> {
        validate_topic(&topic).map_err(ClassicGroupCycleCandidateError::Catalog)?;
        if let Some(topic_id) = self.catalog.topic_id(&topic) {
            return Ok(topic_id);
        }
        if let Some(topic_id) = self.staged.get(&topic) {
            return Ok(*topic_id);
        }
        let actual = self.catalog.retained_topic_count() + self.staged.len() + 1;
        if actual > MAX_GROUP_SESSION_TOPICS {
            return Err(ClassicGroupCycleCandidateError::TopicCapacity {
                actual,
                limit: MAX_GROUP_SESSION_TOPICS,
            });
        }
        let bytes = self
            .retained_topic_name_bytes
            .checked_add(topic.len())
            .ok_or(ClassicGroupCycleCandidateError::Catalog(
                GroupSessionCatalogError::RetainedTopicBytesOverflow,
            ))?;
        if bytes > MAX_GROUP_SESSION_TOPIC_NAME_BYTES {
            return Err(ClassicGroupCycleCandidateError::Catalog(
                GroupSessionCatalogError::RetainedTopicBytes {
                    actual: bytes,
                    limit: MAX_GROUP_SESSION_TOPIC_NAME_BYTES,
                },
            ));
        }
        let topic_id = self
            .next_topic_id
            .ok_or(ClassicGroupCycleCandidateError::TopicIdentityExhausted)?;
        self.next_topic_id = topic_id.get().checked_add(1).map(TopicId::from_raw);
        self.retained_topic_name_bytes = bytes;
        self.staged.insert(topic, topic_id);
        Ok(topic_id)
    }
}
