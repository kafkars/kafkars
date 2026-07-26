//! Validated topic selection for one bounded `DescribeTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Explicit resource selection for one topic-description query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicsSelection {
    /// Describe these unique topic names in caller order.
    Named(Vec<String>),
    /// Describe every topic visible to the authenticated principal.
    All {
        /// Whether core retains broker-marked internal topics in the terminal.
        include_internal: bool,
    },
}

impl DescribeTopicsSelection {
    /// Returns whether an all-topic query retains broker-marked internal topics.
    pub const fn includes_internal_topics(&self) -> bool {
        match self {
            Self::Named(_) => true,
            Self::All { include_internal } => *include_internal,
        }
    }
}

/// Validated policy input for one topic-description RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicsPlan {
    selection: DescribeTopicsSelection,
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
        Ok(Self {
            selection: DescribeTopicsSelection::Named(topics),
        })
    }

    /// Creates an explicit all-topic query.
    pub const fn all(include_internal: bool) -> Self {
        Self {
            selection: DescribeTopicsSelection::All { include_internal },
        }
    }

    /// Returns the exact query selection.
    pub const fn selection(&self) -> &DescribeTopicsSelection {
        &self.selection
    }
}

/// Invalid deterministic `DescribeTopics` input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTopicsPlanError {
    /// An empty name batch is ambiguous with Kafka's all-topic query.
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
