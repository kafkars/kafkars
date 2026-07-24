//! Validated semantic input for one ordered batched `DescribeTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Ordered, validated policy input for one name-based `DescribeTopics` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicsPlan {
    topics: Vec<String>,
}

impl DescribeTopicsPlan {
    /// Validates a nonempty batch of unique, nonempty topic names.
    pub fn new(topics: Vec<String>) -> Result<Self, DescribeTopicsPlanError> {
        if topics.is_empty() {
            return Err(DescribeTopicsPlanError::EmptyBatch);
        }
        let mut names = BTreeSet::new();
        for topic in &topics {
            if topic.is_empty() {
                return Err(DescribeTopicsPlanError::EmptyTopicName);
            }
            if !names.insert(topic.as_str()) {
                return Err(DescribeTopicsPlanError::DuplicateTopic);
            }
        }
        Ok(Self { topics })
    }

    /// Returns topic names in original caller order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }
}

/// Invalid deterministic `DescribeTopics` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTopicsPlanError {
    /// Kafka cannot execute an empty topic-description batch.
    EmptyBatch,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// Topic names in one batch must be unique.
    DuplicateTopic,
}

impl fmt::Display for DescribeTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "DescribeTopics batch is empty",
            Self::EmptyTopicName => "DescribeTopics topic name is empty",
            Self::DuplicateTopic => "DescribeTopics batch contains a duplicate topic",
        })
    }
}

impl std::error::Error for DescribeTopicsPlanError {}
