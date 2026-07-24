//! Engine-owned request values for automatic-assignment `CreatePartitions`.

use kafka_client_core::{
    CreatePartitionsPlan, CreatePartitionsPlanError,
    CreatePartitionsSpecification as CoreSpecification,
};

/// One topic and its requested new total partition count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionIncrease {
    topic: String,
    total_count: i32,
}

impl PartitionIncrease {
    /// Creates one broker-assigned partition increase.
    pub fn new(topic: impl Into<String>, total_count: i32) -> Self {
        Self {
            topic: topic.into(),
            total_count,
        }
    }
}

/// One ordered batch-native `CreatePartitions` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePartitionsRequest {
    topics: Vec<PartitionIncrease>,
    validate_only: bool,
}

impl CreatePartitionsRequest {
    /// Creates one ordered automatic-assignment batch.
    pub const fn new(topics: Vec<PartitionIncrease>) -> Self {
        Self {
            topics,
            validate_only: false,
        }
    }

    /// Selects validation without broker mutation.
    #[must_use]
    pub const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn into_plan(self) -> Result<CreatePartitionsPlan, CreatePartitionsPlanError> {
        CreatePartitionsPlan::new(
            self.topics
                .into_iter()
                .map(|topic| CoreSpecification::new(topic.topic, topic.total_count))
                .collect(),
            self.validate_only,
        )
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for topic in &mut self.topics {
            topic.topic = canonical_string(std::mem::take(&mut topic.topic));
        }
        self.topics = canonical_vec(self.topics);
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let text_bytes = self
            .topics
            .iter()
            .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.topic.len()))?;
        super::retention::request_charge(self.topics.len(), 0, text_bytes)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self
                .topics
                .iter()
                .all(|topic| topic.topic.capacity() == topic.topic.len())
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
