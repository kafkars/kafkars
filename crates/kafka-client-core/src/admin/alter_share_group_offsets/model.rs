//! Validated semantic input for one API-91 share-group offset alteration.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes in the one share-group coordinator identity.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum UTF-8 bytes in one requested topic name.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;
/// Maximum topic-partition alterations retained by one request.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS: usize = 4 * 1024;
/// Maximum aggregate group and topic-name bytes retained by one request.
pub const ALTER_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;

/// One caller-ordered API-91 starting-offset alteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffset {
    topic: String,
    partition: i32,
    start_offset: i64,
}

impl AlterShareGroupOffset {
    /// Creates one inert alteration for validation by the enclosing plan.
    pub const fn new(topic: String, partition: i32, start_offset: i64) -> Self {
        Self {
            topic,
            partition,
            start_offset,
        }
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the nonnegative partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the nonnegative share-group starting offset.
    pub const fn start_offset(&self) -> i64 {
        self.start_offset
    }
}

/// Validated caller-ordered intent for one destructive API-91 request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterShareGroupOffsetsPlan {
    group_id: String,
    changes: Vec<AlterShareGroupOffset>,
}

impl AlterShareGroupOffsetsPlan {
    /// Validates one group and a nonempty caller-ordered unique alteration set.
    pub fn new(
        group_id: String,
        changes: Vec<AlterShareGroupOffset>,
    ) -> Result<Self, AlterShareGroupOffsetsPlanError> {
        if group_id.is_empty() {
            return Err(AlterShareGroupOffsetsPlanError::EmptyGroupId);
        }
        if group_id.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_GROUP_ID_BYTES {
            return Err(AlterShareGroupOffsetsPlanError::GroupIdTooLong);
        }
        if changes.is_empty() {
            return Err(AlterShareGroupOffsetsPlanError::EmptyAlterationBatch);
        }
        if changes.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS {
            return Err(AlterShareGroupOffsetsPlanError::TooManyPartitions);
        }

        let mut text_bytes = group_id.len();
        let mut identities = BTreeSet::new();
        for change in &changes {
            if change.topic.is_empty() {
                return Err(AlterShareGroupOffsetsPlanError::EmptyTopicName);
            }
            if change.topic.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES {
                return Err(AlterShareGroupOffsetsPlanError::TopicNameTooLong);
            }
            if change.partition < 0 {
                return Err(AlterShareGroupOffsetsPlanError::NegativePartition);
            }
            if change.start_offset < 0 {
                return Err(AlterShareGroupOffsetsPlanError::NegativeStartOffset);
            }
            if !identities.insert((change.topic.as_str(), change.partition)) {
                return Err(AlterShareGroupOffsetsPlanError::DuplicateTopicPartition);
            }
            text_bytes = text_bytes
                .checked_add(change.topic.len())
                .ok_or(AlterShareGroupOffsetsPlanError::RequestTextTooLarge)?;
            if text_bytes > ALTER_SHARE_GROUP_OFFSETS_MAX_REQUEST_TEXT_BYTES {
                return Err(AlterShareGroupOffsetsPlanError::RequestTextTooLarge);
            }
        }
        Ok(Self { group_id, changes })
    }

    /// Returns the exact share-group coordinator key.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns alterations in original caller order.
    pub fn changes(&self) -> &[AlterShareGroupOffset] {
        &self.changes
    }
}

/// Invalid deterministic share-group offset alteration intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlterShareGroupOffsetsPlanError {
    /// The request must name one explicit share group.
    EmptyGroupId,
    /// The group identity cannot fit Kafka's string domain.
    GroupIdTooLong,
    /// API 91 cannot alter an empty topic-partition batch.
    EmptyAlterationBatch,
    /// One request cannot retain more than 4096 topic-partitions.
    TooManyPartitions,
    /// Topic names must not be empty.
    EmptyTopicName,
    /// A topic name cannot fit Kafka's string domain.
    TopicNameTooLong,
    /// Partition indices must be nonnegative.
    NegativePartition,
    /// Starting offsets must be nonnegative.
    NegativeStartOffset,
    /// One request cannot repeat a topic-partition identity.
    DuplicateTopicPartition,
    /// Aggregate request text exceeds the one-MiB semantic bound.
    RequestTextTooLarge,
}

impl fmt::Display for AlterShareGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid AlterShareGroupOffsets plan: {self:?}")
    }
}

impl std::error::Error for AlterShareGroupOffsetsPlanError {}
