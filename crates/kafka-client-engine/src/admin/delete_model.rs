//! Engine-owned public request values for one name-based `DeleteTopics` batch.

use kafka_client_core::{DeleteTopicsPlan, DeleteTopicsPlanError};

/// One ordered, batch-native name-based `DeleteTopics` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteTopicsRequest {
    topics: Vec<String>,
}

impl DeleteTopicsRequest {
    /// Creates one ordered batch of topic names.
    pub const fn new(topics: Vec<String>) -> Self {
        Self { topics }
    }

    pub(crate) fn into_plan(self) -> Result<DeleteTopicsPlan, DeleteTopicsPlanError> {
        DeleteTopicsPlan::new(self.topics)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for topic in &mut self.topics {
            *topic = canonical_string(std::mem::take(topic));
        }
        self.topics = canonical_vec(self.topics);
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let text_bytes = self
            .topics
            .iter()
            .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?;
        super::retention::request_charge(self.topics.len(), 0, text_bytes)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self
                .topics
                .iter()
                .all(|topic| topic.capacity() == topic.len())
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
