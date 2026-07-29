//! Engine-owned request selection for one name-or-topic-ID `DeleteTopics` batch.

use kafka_client_core::{DeleteTopicsPlan, DeleteTopicsPlanError};

/// One ordered, batch-native name-or-topic-ID `DeleteTopics` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteTopicsRequest {
    selection: DeleteTopicsRequestSelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteTopicsRequestSelection {
    Named(Vec<String>),
    Ids(Vec<[u8; 16]>),
}

impl DeleteTopicsRequest {
    /// Creates one ordered batch of topic names.
    pub const fn new(topics: Vec<String>) -> Self {
        Self {
            selection: DeleteTopicsRequestSelection::Named(topics),
        }
    }

    /// Creates one ordered batch of topic IDs.
    pub const fn by_ids(topic_ids: Vec<[u8; 16]>) -> Self {
        Self {
            selection: DeleteTopicsRequestSelection::Ids(topic_ids),
        }
    }

    pub(crate) fn into_plan(self) -> Result<DeleteTopicsPlan, DeleteTopicsPlanError> {
        match self.selection {
            DeleteTopicsRequestSelection::Named(topics) => DeleteTopicsPlan::new(topics),
            DeleteTopicsRequestSelection::Ids(topic_ids) => DeleteTopicsPlan::by_ids(topic_ids),
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        match &mut self.selection {
            DeleteTopicsRequestSelection::Named(topics) => {
                for topic in topics.iter_mut() {
                    *topic = canonical_string(std::mem::take(topic));
                }
                *topics = canonical_vec(std::mem::take(topics));
            }
            DeleteTopicsRequestSelection::Ids(topic_ids) => {
                *topic_ids = canonical_vec(std::mem::take(topic_ids));
            }
        }
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        match &self.selection {
            DeleteTopicsRequestSelection::Named(topics) => {
                let text_bytes = topics
                    .iter()
                    .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?;
                super::retention::request_charge(topics.len(), 0, text_bytes)
            }
            DeleteTopicsRequestSelection::Ids(topic_ids) => {
                super::retention::request_charge(topic_ids.len(), 0, 0)
            }
        }
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        match &self.selection {
            DeleteTopicsRequestSelection::Named(topics) => {
                topics.capacity() == topics.len()
                    && topics.iter().all(|topic| topic.capacity() == topic.len())
            }
            DeleteTopicsRequestSelection::Ids(topic_ids) => topic_ids.capacity() == topic_ids.len(),
        }
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
