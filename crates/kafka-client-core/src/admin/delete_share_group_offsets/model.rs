//! Validated semantic input for one API-92 share-group offset deletion.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes in the one share-group coordinator identity.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum UTF-8 bytes in one requested topic name.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;
/// Maximum topics retained by one request.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS: usize = 4 * 1024;
/// Maximum aggregate group and topic-name bytes retained by one request.
pub const DELETE_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;

/// Validated caller-ordered intent for one destructive API-92 request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteShareGroupOffsetsPlan {
    group_id: String,
    topics: Vec<String>,
}

impl DeleteShareGroupOffsetsPlan {
    /// Validates one group and a nonempty caller-ordered unique topic set.
    pub fn new(
        group_id: String,
        topics: Vec<String>,
    ) -> Result<Self, DeleteShareGroupOffsetsPlanError> {
        if group_id.is_empty() {
            return Err(DeleteShareGroupOffsetsPlanError::EmptyGroupId);
        }
        if group_id.len() > DELETE_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES {
            return Err(DeleteShareGroupOffsetsPlanError::GroupIdTooLong);
        }
        if topics.is_empty() {
            return Err(DeleteShareGroupOffsetsPlanError::EmptyTopicBatch);
        }
        if topics.len() > DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS {
            return Err(DeleteShareGroupOffsetsPlanError::TooManyTopics);
        }

        let mut text_bytes = group_id.len();
        let mut unique = BTreeSet::new();
        for topic in &topics {
            if topic.is_empty() {
                return Err(DeleteShareGroupOffsetsPlanError::EmptyTopicName);
            }
            if topic.len() > DELETE_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES {
                return Err(DeleteShareGroupOffsetsPlanError::TopicNameTooLong);
            }
            if !unique.insert(topic.as_str()) {
                return Err(DeleteShareGroupOffsetsPlanError::DuplicateTopicName);
            }
            text_bytes = text_bytes
                .checked_add(topic.len())
                .ok_or(DeleteShareGroupOffsetsPlanError::RequestTextTooLarge)?;
            if text_bytes > DELETE_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES {
                return Err(DeleteShareGroupOffsetsPlanError::RequestTextTooLarge);
            }
        }
        Ok(Self { group_id, topics })
    }

    /// Returns the exact share-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns topic names in original caller order.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }
}

/// Invalid deterministic share-group offset deletion intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteShareGroupOffsetsPlanError {
    /// The request must name one explicit share group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// API 92 cannot delete offsets for an empty topic batch.
    EmptyTopicBatch,
    /// One request cannot contain more than 4096 topics.
    TooManyTopics,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// One request cannot repeat a topic identity.
    DuplicateTopicName,
    /// Aggregate request text exceeds the one-MiB semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for DeleteShareGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DeleteShareGroupOffsets plan: {self:?}")
    }
}

impl std::error::Error for DeleteShareGroupOffsetsPlanError {}
