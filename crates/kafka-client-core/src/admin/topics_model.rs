//! Validated topic selection for one bounded `DescribeTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Explicit resource selection for one topic-description query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTopicsSelection {
    /// Describe these unique topic names in caller order.
    Named(Vec<String>),
    /// Describe these unique nonzero topic IDs in caller order.
    Ids(Vec<[u8; 16]>),
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
            Self::Named(_) | Self::Ids(_) => true,
            Self::All { include_internal } => *include_internal,
        }
    }
}

/// Validated policy input for one topic-description RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicsPlan {
    selection: DescribeTopicsSelection,
    include_authorized_operations: bool,
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
            include_authorized_operations: false,
        })
    }

    /// Validates a nonempty batch of unique, nonzero topic IDs.
    pub fn by_ids(topic_ids: Vec<[u8; 16]>) -> Result<Self, DescribeTopicsPlanError> {
        if topic_ids.is_empty() {
            return Err(DescribeTopicsPlanError::EmptyBatch);
        }
        let mut ids = BTreeSet::new();
        for topic_id in &topic_ids {
            if *topic_id == [0; 16] {
                return Err(DescribeTopicsPlanError::ZeroTopicId);
            }
            if !ids.insert(*topic_id) {
                return Err(DescribeTopicsPlanError::DuplicateTopicId);
            }
        }
        Ok(Self {
            selection: DescribeTopicsSelection::Ids(topic_ids),
            include_authorized_operations: false,
        })
    }

    /// Creates an explicit all-topic query.
    pub const fn all(include_internal: bool) -> Self {
        Self {
            selection: DescribeTopicsSelection::All { include_internal },
            include_authorized_operations: false,
        }
    }

    /// Selects whether Kafka should return the caller's topic authorization bitfield.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    /// Returns the exact query selection.
    pub const fn selection(&self) -> &DescribeTopicsSelection {
        &self.selection
    }

    /// Returns whether topic authorization bitfields were requested.
    pub const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
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
    /// The all-zero protocol sentinel is not a topic identity.
    ZeroTopicId,
    /// Topic IDs in one batch must be unique.
    DuplicateTopicId,
}

impl fmt::Display for DescribeTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "DescribeTopics batch is empty",
            Self::EmptyTopicName => "DescribeTopics topic name is empty",
            Self::DuplicateTopic => "DescribeTopics batch contains a duplicate topic",
            Self::ZeroTopicId => "DescribeTopics topic ID is the all-zero sentinel",
            Self::DuplicateTopicId => "DescribeTopics batch contains a duplicate topic ID",
        })
    }
}

impl std::error::Error for DescribeTopicsPlanError {}
