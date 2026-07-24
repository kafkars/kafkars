//! Engine-owned public request values for one name-based `DescribeTopics` batch.

use kafka_client_core::{DescribeTopicsPlan, DescribeTopicsPlanError};

const RESULT_BYTES_PER_TOPIC: usize = 128 * 1024;

/// One ordered, batch-native name-based `DescribeTopics` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicsRequest {
    topics: Vec<String>,
}

impl DescribeTopicsRequest {
    /// Creates one ordered batch of topic names.
    pub const fn new(topics: Vec<String>) -> Self {
        Self { topics }
    }

    pub(crate) fn into_plan(self) -> Result<DescribeTopicsPlan, DescribeTopicsPlanError> {
        DescribeTopicsPlan::new(self.topics)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for topic in &mut self.topics {
            *topic = canonical_string(std::mem::take(topic));
        }
        self.topics = self.topics.into_boxed_slice().into_vec();
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let text_bytes = self
            .topics
            .iter()
            .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?;
        let request = super::retention::request_charge(self.topics.len(), 0, text_bytes)?;
        request.checked_add(self.topics.len().checked_mul(RESULT_BYTES_PER_TOPIC)?)
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
