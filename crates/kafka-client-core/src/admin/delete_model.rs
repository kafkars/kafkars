//! Validated semantic input for one ordered batched `DeleteTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Ordered, validated policy input for one name-based `DeleteTopics` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsPlan {
    topics: Vec<String>,
}

impl DeleteTopicsPlan {
    /// Validates a nonempty batch of unique, nonempty topic names.
    pub fn new(topics: Vec<String>) -> Result<Self, DeleteTopicsPlanError> {
        if topics.is_empty() {
            return Err(DeleteTopicsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        for topic in &topics {
            if topic.is_empty() {
                return Err(DeleteTopicsPlanError::EmptyTopicName);
            }
            if !names.insert(topic.as_str()) {
                return Err(DeleteTopicsPlanError::DuplicateTopic);
            }
        }
        Ok(Self { topics })
    }

    /// Returns topic names in original caller order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }
}

/// Invalid deterministic `DeleteTopics` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteTopicsPlanError {
    /// Kafka cannot execute an empty topic-deletion batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
}

impl fmt::Display for DeleteTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "DeleteTopics batch is empty",
            Self::EmptyTopicName => "DeleteTopics topic name is empty",
            Self::DuplicateTopic => "DeleteTopics batch contains a duplicate topic",
        })
    }
}

impl std::error::Error for DeleteTopicsPlanError {}
