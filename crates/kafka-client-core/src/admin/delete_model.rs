//! Validated name-or-topic-ID input for one ordered batched `DeleteTopics` operation.

use core::fmt;
use std::collections::BTreeSet;

/// Explicit identity selection for one destructive topic-deletion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTopicsSelection {
    /// Delete these unique topic names in caller order.
    Named(Vec<String>),
    /// Delete these unique nonzero topic IDs in caller order.
    Ids(Vec<[u8; 16]>),
}

/// Ordered, validated policy input for one `DeleteTopics` RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsPlan {
    selection: DeleteTopicsSelection,
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
        Ok(Self {
            selection: DeleteTopicsSelection::Named(topics),
        })
    }

    /// Validates a nonempty batch of unique, nonzero topic IDs.
    pub fn by_ids(topic_ids: Vec<[u8; 16]>) -> Result<Self, DeleteTopicsPlanError> {
        if topic_ids.is_empty() {
            return Err(DeleteTopicsPlanError::EmptyBatch);
        }
        let mut ids = BTreeSet::new();
        for topic_id in &topic_ids {
            if *topic_id == [0; 16] {
                return Err(DeleteTopicsPlanError::ZeroTopicId);
            }
            if !ids.insert(*topic_id) {
                return Err(DeleteTopicsPlanError::DuplicateTopicId);
            }
        }
        Ok(Self {
            selection: DeleteTopicsSelection::Ids(topic_ids),
        })
    }

    /// Returns topic names in original caller order.
    pub fn topics(&self) -> &[String] {
        match &self.selection {
            DeleteTopicsSelection::Named(topics) => topics,
            DeleteTopicsSelection::Ids(_) => &[],
        }
    }

    /// Returns topic IDs in original caller order.
    pub fn topic_ids(&self) -> &[[u8; 16]] {
        match &self.selection {
            DeleteTopicsSelection::Named(_) => &[],
            DeleteTopicsSelection::Ids(topic_ids) => topic_ids,
        }
    }

    /// Returns the validated identity selection.
    pub const fn selection(&self) -> &DeleteTopicsSelection {
        &self.selection
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
    /// The all-zero protocol sentinel is not a topic identity.
    ZeroTopicId,
    /// Topic IDs in one batch must be unique.
    DuplicateTopicId,
}

impl fmt::Display for DeleteTopicsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyBatch => "DeleteTopics batch is empty",
            Self::EmptyTopicName => "DeleteTopics topic name is empty",
            Self::DuplicateTopic => "DeleteTopics batch contains a duplicate topic",
            Self::ZeroTopicId => "DeleteTopics topic ID is the all-zero sentinel",
            Self::DuplicateTopicId => "DeleteTopics batch contains a duplicate topic ID",
        })
    }
}

impl std::error::Error for DeleteTopicsPlanError {}
